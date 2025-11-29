from typing import Optional
from pyspark.sql import SparkSession
from .base import BasePipeline
from .config_loader import ConfigLoader
from .pipelines.bronze_raw_ingestion import BronzeRawIngestionPipeline
from .pipelines.bronze_streaming_ingestion import BronzeStreamingIngestionPipeline
from .pipelines.silver_batch_transformation import SilverBatchTransformationPipeline
from .pipelines.silver_streaming_transformation import SilverStreamingTransformationPipeline
from .pipelines.gold_daily_aggregate import GoldDailyAggregatePipeline
from .pipelines.gold_realtime_dashboard import GoldRealtimeDashboardPipeline
from .pipelines.dimension_daily_scd2 import DimensionDailySCD2Pipeline
from .pipelines.fact_daily_incremental import FactDailyIncrementalPipeline
from .pipelines.feature_store_daily_materialization import FeatureStoreDailyMaterializationPipeline
from .pipelines.feature_store_realtime_sync import FeatureStoreRealtimeSyncPipeline
from .pipelines.ml_training_champion_challenger import MLTrainingChampionChallengerPipeline
from .pipelines.ml_batch_scoring import MLBatchScoringPipeline
from .pipelines.ml_realtime_scoring import MLRealtimeScoringPipeline
from .pipelines.model_drift_monitoring import ModelDriftMonitoringPipeline
from .pipelines.data_quality_expectations import DataQualityExpectationsPipeline
from .pipelines.data_contract_enforcement import DataContractEnforcementPipeline
from .pipelines.backfill_historical import BackfillHistoricalPipeline
from .pipelines.catch_up_after_outage import CatchUpAfterOutagePipeline
from .pipelines.table_reconciliation import TableReconciliationPipeline
from .pipelines.vacuum_and_optimize_maintenance import VacuumAndOptimizeMaintenancePipeline
from .pipelines.zorder_and_analyze import ZOrderAndAnalyzePipeline
from .pipelines.unity_catalog_governance_sync import UnityCatalogGovernanceSyncPipeline
from .pipelines.cost_attribution_tagging import CostAttributionTaggingPipeline
from .pipelines.disaster_recovery_replication import DisasterRecoveryReplicationPipeline

class PipelineFactory:
    """
    Factory to create pipeline instances.
    """
    _registry = {
        "BronzeRawIngestionPipeline": BronzeRawIngestionPipeline,
        "BronzeStreamingIngestionPipeline": BronzeStreamingIngestionPipeline,
        "SilverBatchTransformationPipeline": SilverBatchTransformationPipeline,
        "SilverStreamingTransformationPipeline": SilverStreamingTransformationPipeline,
        "GoldDailyAggregatePipeline": GoldDailyAggregatePipeline,
        "GoldRealtimeDashboardPipeline": GoldRealtimeDashboardPipeline,
        "DimensionDailySCD2Pipeline": DimensionDailySCD2Pipeline,
        "FactDailyIncrementalPipeline": FactDailyIncrementalPipeline,
        "FeatureStoreDailyMaterializationPipeline": FeatureStoreDailyMaterializationPipeline,
        "FeatureStoreRealtimeSyncPipeline": FeatureStoreRealtimeSyncPipeline,
        "MLTrainingChampionChallengerPipeline": MLTrainingChampionChallengerPipeline,
        "MLBatchScoringPipeline": MLBatchScoringPipeline,
        "MLRealtimeScoringPipeline": MLRealtimeScoringPipeline,
        "ModelDriftMonitoringPipeline": ModelDriftMonitoringPipeline,
        "DataQualityExpectationsPipeline": DataQualityExpectationsPipeline,
        "DataContractEnforcementPipeline": DataContractEnforcementPipeline,
        "BackfillHistoricalPipeline": BackfillHistoricalPipeline,
        "CatchUpAfterOutagePipeline": CatchUpAfterOutagePipeline,
        "TableReconciliationPipeline": TableReconciliationPipeline,
        "VacuumAndOptimizeMaintenancePipeline": VacuumAndOptimizeMaintenancePipeline,
        "ZOrderAndAnalyzePipeline": ZOrderAndAnalyzePipeline,
        "UnityCatalogGovernanceSyncPipeline": UnityCatalogGovernanceSyncPipeline,
        "CostAttributionTaggingPipeline": CostAttributionTaggingPipeline,
        "DisasterRecoveryReplicationPipeline": DisasterRecoveryReplicationPipeline
    }

    @classmethod
    def get(cls, pipeline_name: str, env: str = "prod", spark: Optional[SparkSession] = None) -> BasePipeline:
        """
        Get a fully configured pipeline instance.
        """
        loader = ConfigLoader()
        config = loader.load_config(pipeline_name, env)
        
        pipeline_class = cls._registry.get(config.pipeline_type)
        if not pipeline_class:
            raise ValueError(f"Unknown pipeline type: {config.pipeline_type}")
            
        pipeline = pipeline_class(config, spark)
        return pipeline.build()
