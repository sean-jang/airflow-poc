from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


class GoogleAnalyticsOperator(BaseOperator):
    def __init__(self,
                 key_file: str, 
                 view_id: str,
                 dimensions: str,
                 metrics: str,
                 metricAggregations: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.key_file = key_file
        self.view_id = view_id
        self.dimensions = dimensions
        self.metrics = metrics
        self.metricAggregations = metricAggregations
    
    def execute(self, context):
        scope = "https://www.googleapis.com/auth/analytics.readonly"
        credentials = Credentials.from_service_account_file(
            self.key_file, scopes=[scope]
        )
        ga4_service = build("analyticsdata", "v1beta", credentials=credentials)
        execution_date = datetime.strftime(context['execution_date'], '%Y-%m-%d')[:10]
        response = (
            ga4_service.properties()
            .runReport(
                property=f"properties/{self.view_id}",
                body= {
                    "dimensions": [{"name": f"{self.dimensions}"}],
                    "metrics":[{"name": f"{self.metrics}"}],
                    "dateRanges":[{"startDate": execution_date,"endDate": execution_date}],
                    "metricAggregations":[f"{self.metricAggregations}"]
                }
            )
            .execute()
        )

        result_dict = dict()
        for row in response['rows']:
            dimension = row['dimensionValues'][0]['value']
            metric = row['metricValues'][0]['value']
            result_dict[dimension] = metric
        
        return result_dict