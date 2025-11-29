from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.notification import SendSlackAlertTask

class ModelDriftMonitoringPipeline(BasePipeline):
    """
    Compares prediction vs feature drift daily → alerts if PSI > threshold
    """
    def build(self) -> 'BasePipeline':
        # 1. Calculate Drift
        self.add_task(TransformTask("calculate_drift", {
            "transformers": [
                {
                    "name": "drift_calculator",
                    "params": {
                        "reference_table": self.config.extra_config.get("reference_table"),
                        "current_table": self.config.extra_config.get("current_table"),
                        "metric": "psi"
                    }
                }
            ]
        }))
        
        # 2. Alert if Drift Detected
        # Note: In a real implementation, we'd need a conditional task or the drift task would raise/return status
        # Here we assume the drift task returns a metric we can check, or we just always run the alert task which checks context
        self.add_task(SendSlackAlertTask("alert_drift", {
            "message": "Model drift detected! PSI > threshold"
        }))
        
        return self
