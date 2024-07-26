import great_expectations as ge
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig


class Expectator : 
    def __init__(self, project_name: str = "great_expectations"):
        # Define your Databricks File System (DBFS) paths
        self._ge_root_dir = f"/dbfs/path/to/{project_name}"
        self._checkpoint_store_path = f"/dbfs/path/to/{project_name}/checkpoints"
        self.context = self.create_context()
        
    def load_config(self) -> DataContextConfig: 
        # Create the configuration for Great Expectations
        return DataContextConfig(
            config_version=2,
            plugins_directory=None,
            stores={
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "FilesystemStoreBackend",
                        "base_directory": f"{self._ge_root_dir}/expectations",
                    },
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "FilesystemStoreBackend",
                        "base_directory": f"{self._ge_root_dir}/validations",
                    },
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "FilesystemStoreBackend",
                        "base_directory": self._checkpoint_store_path,
                    },
                },
            },
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            checkpoint_store_name="checkpoint_store",
            data_docs_sites={
                "local_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "FilesystemStoreBackend",
                        "base_directory": f"{self._ge_root_dir}/data_docs/local_site/",
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                        "show_cta_footer": True,
                    },
                },
            },
            anonymous_usage_statistics={
                "enabled": True,
            },
        )

    def create_context(self):
        # Create a new DataContext
        return ge.DataContext(project_config = self.load_config())

    def get_context(self):
        return self.context