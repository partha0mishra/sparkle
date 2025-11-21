"""
Service for Git operations using GitPython.
Handles commit, push, pull, branch management, and PR creation.
"""
import os
from pathlib import Path
from typing import Optional
from datetime import datetime
import requests

try:
    from git import Repo, GitCommandError, InvalidGitRepositoryError
    GIT_AVAILABLE = True
except ImportError:
    Repo = None
    GitCommandError = Exception
    InvalidGitRepositoryError = Exception
    GIT_AVAILABLE = False

from core.config import settings
from schemas.git import (
    GitBranch,
    GitStatus,
    GitCommitResponse,
    GitPullResponse,
    GitPRResponse,
)


class GitService:
    """Service for Git operations."""

    def __init__(self):
        self.repo_path = Path(settings.GIT_REPO_PATH)
        self._repo: Optional[Repo] = None

    def _get_repo(self) -> Repo:
        """Get or initialize Git repository."""
        if not GIT_AVAILABLE:
            raise RuntimeError("GitPython is not available")

        if self._repo is None:
            try:
                self._repo = Repo(self.repo_path)
            except InvalidGitRepositoryError:
                # Initialize new repository
                self.repo_path.mkdir(parents=True, exist_ok=True)
                self._repo = Repo.init(self.repo_path)

                # Set up remote if configured
                if settings.GIT_REMOTE_URL:
                    try:
                        self._repo.create_remote("origin", settings.GIT_REMOTE_URL)
                    except Exception:
                        pass  # Remote already exists

        return self._repo

    def get_status(self) -> GitStatus:
        """Get current Git status."""
        repo = self._get_repo()

        # Get current branch
        try:
            current_branch = repo.active_branch.name
        except Exception:
            current_branch = "HEAD"

        # Check if working tree is clean
        is_clean = not repo.is_dirty(untracked_files=True)

        # Get modified, untracked, and staged files
        modified_files = [item.a_path for item in repo.index.diff(None)]
        untracked_files = repo.untracked_files
        staged_files = [item.a_path for item in repo.index.diff("HEAD")]

        # Get ahead/behind counts
        ahead = 0
        behind = 0
        try:
            if repo.remotes:
                origin = repo.remotes.origin
                origin.fetch()
                ahead = len(list(repo.iter_commits(f"origin/{current_branch}..{current_branch}")))
                behind = len(list(repo.iter_commits(f"{current_branch}..origin/{current_branch}")))
        except Exception:
            pass

        # Get remote URL
        remote_url = None
        try:
            if repo.remotes:
                remote_url = repo.remotes.origin.url
        except Exception:
            pass

        return GitStatus(
            current_branch=current_branch,
            is_clean=is_clean,
            modified_files=modified_files,
            untracked_files=untracked_files,
            staged_files=staged_files,
            ahead=ahead,
            behind=behind,
            remote_url=remote_url,
        )

    def get_branches(self) -> list[GitBranch]:
        """Get list of all branches."""
        repo = self._get_repo()
        branches = []

        try:
            current_branch_name = repo.active_branch.name
        except Exception:
            current_branch_name = None

        for branch in repo.branches:
            is_current = branch.name == current_branch_name

            # Get last commit info
            commit = branch.commit
            branches.append(
                GitBranch(
                    name=branch.name,
                    is_current=is_current,
                    commit_sha=commit.hexsha,
                    last_commit_message=commit.message.strip(),
                    last_commit_date=datetime.fromtimestamp(commit.committed_date),
                )
            )

        return branches

    def commit(
        self,
        message: str,
        files: Optional[list[str]] = None,
        author_name: str = "Sparkle Studio",
        author_email: str = "studio@sparkle.dev",
    ) -> GitCommitResponse:
        """Commit changes to Git."""
        repo = self._get_repo()

        # Stage files
        if files:
            repo.index.add(files)
        else:
            repo.git.add(A=True)  # Add all changes

        # Get list of files being committed
        files_changed = [item.a_path for item in repo.index.diff("HEAD")]

        # Commit
        commit = repo.index.commit(
            message,
            author=f"{author_name} <{author_email}>",
        )

        return GitCommitResponse(
            commit_sha=commit.hexsha,
            message=commit.message.strip(),
            author=f"{author_name} <{author_email}>",
            timestamp=datetime.fromtimestamp(commit.committed_date),
            files_changed=files_changed,
            pushed=False,
        )

    def push(self, branch: Optional[str] = None) -> bool:
        """Push commits to remote."""
        repo = self._get_repo()

        if not repo.remotes:
            raise RuntimeError("No remote configured")

        origin = repo.remotes.origin

        # Set up authentication if token provided
        if settings.GIT_TOKEN and settings.GIT_USERNAME:
            # Update remote URL with token
            remote_url = origin.url
            if "github.com" in remote_url:
                auth_url = remote_url.replace(
                    "https://",
                    f"https://{settings.GIT_USERNAME}:{settings.GIT_TOKEN}@"
                )
                origin.set_url(auth_url)

        # Push
        if branch:
            origin.push(branch)
        else:
            origin.push()

        return True

    def pull(self, branch: Optional[str] = None, rebase: bool = False) -> GitPullResponse:
        """Pull from remote."""
        repo = self._get_repo()

        if not repo.remotes:
            raise RuntimeError("No remote configured")

        origin = repo.remotes.origin
        current_branch = repo.active_branch.name if branch is None else branch

        # Fetch first
        origin.fetch()

        # Get commits before pull
        commits_before = list(repo.iter_commits(current_branch, max_count=100))

        # Pull
        if rebase:
            repo.git.pull("origin", current_branch, rebase=True)
        else:
            origin.pull(current_branch)

        # Get commits after pull
        commits_after = list(repo.iter_commits(current_branch, max_count=100))
        commits_pulled = len(commits_after) - len(commits_before)

        # Get changed files
        files_changed = [item.a_path for item in repo.index.diff("HEAD~1")]

        return GitPullResponse(
            success=True,
            branch=current_branch,
            commits_pulled=commits_pulled,
            files_changed=files_changed,
            message=f"Pulled {commits_pulled} commits",
        )

    def create_pull_request(
        self,
        title: str,
        description: str,
        source_branch: str,
        target_branch: str = "main",
        draft: bool = False,
    ) -> GitPRResponse:
        """
        Create a pull request using GitHub/GitLab API.
        Requires GIT_TOKEN and GIT_REMOTE_URL to be configured.
        """
        if not settings.GIT_TOKEN or not settings.GIT_REMOTE_URL:
            raise RuntimeError("Git token and remote URL must be configured")

        remote_url = settings.GIT_REMOTE_URL

        # Determine platform (GitHub or GitLab)
        if "github.com" in remote_url:
            return self._create_github_pr(
                title, description, source_branch, target_branch, draft
            )
        elif "gitlab.com" in remote_url:
            return self._create_gitlab_pr(
                title, description, source_branch, target_branch, draft
            )
        else:
            raise RuntimeError("Unsupported Git platform")

    def _create_github_pr(
        self, title: str, description: str, source: str, target: str, draft: bool
    ) -> GitPRResponse:
        """Create GitHub pull request."""
        # Extract owner/repo from URL
        parts = settings.GIT_REMOTE_URL.replace(".git", "").split("/")
        owner, repo = parts[-2], parts[-1]

        url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        headers = {
            "Authorization": f"token {settings.GIT_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        data = {
            "title": title,
            "body": description,
            "head": source,
            "base": target,
            "draft": draft,
        }

        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()

        pr_data = response.json()
        return GitPRResponse(
            pr_number=pr_data["number"],
            pr_url=pr_data["html_url"],
            title=pr_data["title"],
            source_branch=source,
            target_branch=target,
        )

    def _create_gitlab_pr(
        self, title: str, description: str, source: str, target: str, draft: bool
    ) -> GitPRResponse:
        """Create GitLab merge request."""
        # Extract project ID from URL
        parts = settings.GIT_REMOTE_URL.replace(".git", "").split("/")
        project_path = "/".join(parts[-2:])

        url = f"https://gitlab.com/api/v4/projects/{project_path.replace('/', '%2F')}/merge_requests"
        headers = {
            "PRIVATE-TOKEN": settings.GIT_TOKEN,
        }
        data = {
            "title": title,
            "description": description,
            "source_branch": source,
            "target_branch": target,
            "draft": draft,
        }

        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()

        mr_data = response.json()
        return GitPRResponse(
            pr_number=mr_data["iid"],
            pr_url=mr_data["web_url"],
            title=mr_data["title"],
            source_branch=source,
            target_branch=target,
        )


# Global git service instance
git_service = GitService()
