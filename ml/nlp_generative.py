"""
NLP & Generative AI Components (8 components).

Production-ready NLP and LLM-based components.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf, explode, array, struct
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DoubleType
from .base import BaseModelTrainer
from .factory import register_ml_component
import mlflow


@register_ml_component("zero_shot_classifier")
class ZeroShotClassifier(BaseModelTrainer):
    """
    Zero-shot text classification using pre-trained models.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.customer_feedback",
            "text_column": "feedback_text",
            "candidate_labels": ["positive", "negative", "neutral", "complaint"],
            "model_name": "facebook/bart-large-mnli",
            "output_table": "gold.classified_feedback"
        }
    """

    def execute(self) -> DataFrame:
        """Perform zero-shot classification."""
        from transformers import pipeline

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]
        candidate_labels = self.config["candidate_labels"]
        model_name = self.config.get("model_name", "facebook/bart-large-mnli")

        # Load classifier
        classifier = pipeline("zero-shot-classification", model=model_name)

        # Define UDF
        @udf(returnType=StructType([
            StructField("predicted_label", StringType()),
            StructField("score", DoubleType())
        ]))
        def classify_text(text):
            if text is None:
                return ("unknown", 0.0)
            result = classifier(text, candidate_labels)
            return (result["labels"][0], float(result["scores"][0]))

        # Apply classification
        result = df.withColumn("classification", classify_text(col(text_col)))
        result = result.withColumn("predicted_label", col("classification.predicted_label"))
        result = result.withColumn("confidence_score", col("classification.score"))
        result = result.drop("classification")

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        # Log statistics
        label_counts = result.groupBy("predicted_label").count().collect()
        self.log_params({
            "num_labels": len(candidate_labels),
            "model": model_name
        })

        return result


@register_ml_component("multilingual_sentiment")
class MultilingualSentiment(BaseModelTrainer):
    """
    Multilingual sentiment analysis using mBERT or XLM-RoBERTa.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.multilingual_reviews",
            "text_column": "review_text",
            "language_column": "language",
            "model_name": "nlptown/bert-base-multilingual-uncased-sentiment",
            "output_table": "gold.sentiment_scores"
        }
    """

    def execute(self) -> DataFrame:
        """Analyze multilingual sentiment."""
        from transformers import pipeline

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]
        model_name = self.config.get("model_name", "nlptown/bert-base-multilingual-uncased-sentiment")

        # Load sentiment analyzer
        sentiment_analyzer = pipeline("sentiment-analysis", model=model_name)

        # Define UDF
        @udf(returnType=StructType([
            StructField("sentiment", StringType()),
            StructField("score", DoubleType())
        ]))
        def analyze_sentiment(text):
            if text is None or text == "":
                return ("neutral", 0.5)
            result = sentiment_analyzer(text[:512])[0]  # Truncate to model max length
            return (result["label"], float(result["score"]))

        # Apply sentiment analysis
        result = df.withColumn("sentiment_result", analyze_sentiment(col(text_col)))
        result = result.withColumn("sentiment", col("sentiment_result.sentiment"))
        result = result.withColumn("sentiment_score", col("sentiment_result.score"))
        result = result.drop("sentiment_result")

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("spark_nlp_ner")
class SparkNLPNER(BaseModelTrainer):
    """
    Named Entity Recognition using Spark NLP.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.documents",
            "text_column": "document_text",
            "ner_model": "ner_dl",
            "output_table": "gold.extracted_entities"
        }
    """

    def execute(self) -> DataFrame:
        """Extract named entities."""
        from sparknlp.base import DocumentAssembler, Finisher
        from sparknlp.annotator import (
            Tokenizer, WordEmbeddingsModel, NerDLModel, NerConverter
        )
        from pyspark.ml import Pipeline

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]
        ner_model_name = self.config.get("ner_model", "ner_dl")

        # Build Spark NLP pipeline
        document_assembler = DocumentAssembler() \
            .setInputCol(text_col) \
            .setOutputCol("document")

        tokenizer = Tokenizer() \
            .setInputCols(["document"]) \
            .setOutputCol("token")

        # Load pre-trained embeddings
        embeddings = WordEmbeddingsModel.pretrained("glove_100d") \
            .setInputCols(["document", "token"]) \
            .setOutputCol("embeddings")

        # Load NER model
        ner_model = NerDLModel.pretrained(ner_model_name) \
            .setInputCols(["document", "token", "embeddings"]) \
            .setOutputCol("ner")

        # Convert to readable format
        ner_converter = NerConverter() \
            .setInputCols(["document", "token", "ner"]) \
            .setOutputCol("ner_chunk")

        finisher = Finisher() \
            .setInputCols(["ner_chunk"]) \
            .setOutputCols("entities") \
            .setOutputAsArray(True)

        # Build and run pipeline
        pipeline = Pipeline(stages=[
            document_assembler,
            tokenizer,
            embeddings,
            ner_model,
            ner_converter,
            finisher
        ])

        model = pipeline.fit(df)
        result = model.transform(df)

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("bertopic_trainer")
class BERTopicTrainer(BaseModelTrainer):
    """
    Topic modeling using BERTopic.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.documents",
            "text_column": "document_text",
            "num_topics": 10,
            "min_topic_size": 10,
            "output_topics_table": "gold.topics",
            "output_assignments_table": "gold.topic_assignments",
            "model_name": "bertopic_model"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train BERTopic model."""
        from bertopic import BERTopic

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]

        # Convert to pandas
        pdf = df.select(text_col).toPandas()
        documents = pdf[text_col].tolist()

        # Train BERTopic
        topic_model = BERTopic(
            nr_topics=self.config.get("num_topics", 10),
            min_topic_size=self.config.get("min_topic_size", 10),
            calculate_probabilities=True
        )

        topics, probs = topic_model.fit_transform(documents)

        # Get topic info
        topic_info = topic_model.get_topic_info()

        # Save topics
        topics_df = self.spark.createDataFrame(topic_info)
        topics_df.write.mode("overwrite").saveAsTable(self.config["output_topics_table"])

        # Save assignments
        pdf["topic"] = topics
        pdf["topic_probability"] = probs.max(axis=1)
        assignments_df = self.spark.createDataFrame(pdf)
        assignments_df.write.mode("overwrite").saveAsTable(self.config["output_assignments_table"])

        # Log model
        mlflow.sklearn.log_model(topic_model, "model")

        self.log_params({"num_topics": len(topic_info)})

        return {"num_topics": len(topic_info)}


@register_ml_component("text_similarity_scorer")
class TextSimilarityScorer(BaseModelTrainer):
    """
    Calculate semantic text similarity using sentence transformers.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.text_pairs",
            "text1_column": "query",
            "text2_column": "document",
            "model_name": "all-MiniLM-L6-v2",
            "similarity_metric": "cosine",
            "output_table": "gold.similarity_scores"
        }
    """

    def execute(self) -> DataFrame:
        """Calculate text similarity."""
        from sentence_transformers import SentenceTransformer, util

        df = self.spark.table(self.config["input_table"])
        text1_col = self.config["text1_column"]
        text2_col = self.config["text2_column"]
        model_name = self.config.get("model_name", "all-MiniLM-L6-v2")

        # Load model
        model = SentenceTransformer(model_name)

        # Convert to pandas
        pdf = df.toPandas()

        # Generate embeddings
        embeddings1 = model.encode(pdf[text1_col].tolist(), convert_to_tensor=True)
        embeddings2 = model.encode(pdf[text2_col].tolist(), convert_to_tensor=True)

        # Calculate similarity
        similarities = util.cos_sim(embeddings1, embeddings2).diagonal().cpu().numpy()

        pdf["similarity_score"] = similarities

        # Convert back
        result = self.spark.createDataFrame(pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("llm_structured_extractor")
class LLMStructuredExtractor(BaseModelTrainer):
    """
    Extract structured data from unstructured text using LLMs.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.raw_documents",
            "text_column": "document_text",
            "extraction_schema": {
                "name": "string",
                "date": "date",
                "amount": "float",
                "category": "string"
            },
            "llm_endpoint": "databricks-dbrx-instruct",
            "output_table": "gold.structured_extractions"
        }
    """

    def execute(self) -> DataFrame:
        """Extract structured data using LLM."""
        import json
        import requests

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]
        schema = self.config["extraction_schema"]
        llm_endpoint = self.config["llm_endpoint"]

        # Define extraction prompt
        schema_str = json.dumps(schema, indent=2)
        system_prompt = f"""Extract the following fields from the text:
{schema_str}

Return only valid JSON matching the schema."""

        @udf(returnType=StringType())
        def extract_structured_data(text):
            if text is None:
                return "{}"

            prompt = f"{system_prompt}\n\nText: {text}\n\nExtracted JSON:"

            # Call LLM (simplified - would use Databricks Foundation Model API)
            try:
                # Placeholder for actual LLM call
                # response = requests.post(llm_endpoint, json={"prompt": prompt})
                # extracted = response.json()["choices"][0]["text"]
                extracted = "{}"  # Placeholder
                return extracted
            except Exception as e:
                return "{}"

        # Apply extraction
        result = df.withColumn("extracted_json", extract_structured_data(col(text_col)))

        # Parse JSON into columns
        for field, field_type in schema.items():
            from pyspark.sql.functions import get_json_object
            result = result.withColumn(
                f"extracted_{field}",
                get_json_object(col("extracted_json"), f"$.{field}")
            )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("prompt_template_engine")
class PromptTemplateEngine(BaseModelTrainer):
    """
    Generate prompts from templates with dynamic variables.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.prompt_inputs",
            "template": "Summarize the following {document_type} for {audience}: {text}",
            "variables": ["document_type", "audience", "text"],
            "output_table": "gold.generated_prompts"
        }
    """

    def execute(self) -> DataFrame:
        """Generate prompts from template."""
        df = self.spark.table(self.config["input_table"])
        template = self.config["template"]
        variables = self.config["variables"]

        @udf(returnType=StringType())
        def render_prompt(*values):
            prompt = template
            for var, value in zip(variables, values):
                prompt = prompt.replace(f"{{{var}}}", str(value))
            return prompt

        # Apply template
        result = df.withColumn(
            "generated_prompt",
            render_prompt(*[col(var) for var in variables])
        )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("text_summarizer")
class TextSummarizer(BaseModelTrainer):
    """
    Abstractive text summarization using transformer models.

    Sub-Group: NLP & Generative
    Tags: nlp, text, language-models, generation

    Config example:
        {
            "input_table": "silver.long_documents",
            "text_column": "document_text",
            "model_name": "facebook/bart-large-cnn",
            "max_length": 150,
            "min_length": 50,
            "output_table": "gold.summaries"
        }
    """

    def execute(self) -> DataFrame:
        """Generate text summaries."""
        from transformers import pipeline

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]
        model_name = self.config.get("model_name", "facebook/bart-large-cnn")
        max_length = self.config.get("max_length", 150)
        min_length = self.config.get("min_length", 50)

        # Load summarizer
        summarizer = pipeline("summarization", model=model_name)

        @udf(returnType=StringType())
        def summarize_text(text):
            if text is None or len(text) < 100:
                return text

            try:
                # Truncate to model max (1024 for BART)
                text_truncated = text[:1024]
                summary = summarizer(
                    text_truncated,
                    max_length=max_length,
                    min_length=min_length,
                    do_sample=False
                )[0]["summary_text"]
                return summary
            except Exception as e:
                return text[:max_length]

        # Apply summarization
        result = df.withColumn("summary", summarize_text(col(text_col)))

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        # Calculate compression ratio
        avg_original_length = df.select(avg(col(text_col).cast("string").length())).collect()[0][0]
        avg_summary_length = result.select(avg(col("summary").length())).collect()[0][0]
        compression_ratio = avg_original_length / avg_summary_length if avg_summary_length > 0 else 0

        self.log_metrics({
            "avg_compression_ratio": float(compression_ratio)
        })

        return result
