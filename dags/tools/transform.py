from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class Transformer(ABC):
    @staticmethod
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame :
        ...


class AppointmentsTransformer(Transformer):
    def transform(df, next_ds):
        df["status"] = df["status"].map(
            lambda x: 1 if x in ["completed", "confirmed"] else 0
        )
        df_summary = pd.DataFrame(
            {
                "date_id": next_ds,
                "mobileAppointments_requests_web": np.nan,
                "mobileAppointments_requests_web_users": np.nan,
                "mobileAppointments_requests_app": len(df),
                "mobileAppointments_requests_app_users": df["userId"].nunique(),
                "mobileAppointments_requests_completed": df["status"].sum(),
                "mobileAppointments_hospitals_active": df["hospitalId"].nunique()
            }, index=[0]
        )
        return df_summary

class ChartReceiptsTransformer(Transformer):
    def transform(df, next_ds):
        df_summary = pd.DataFrame(
            {"date_id": next_ds, "receipt_requests_dummy": len(df)}, index=[0]
        )
        return df_summary

class ReceiptsTransformer(Transformer):
    def transform(df, next_ds):
        df["receiptStatus"] = df["receiptStatus"].map(lambda x: 1 if x == "F05" else 0)
        df = df.groupby("sourceType").agg(
            {
                "id": "size",
                "hospitalId": "nunique",
                "receiptPatientId": "nunique",
                "receiptStatus": "sum"
            }
        )
        df_summary = pd.DataFrame(
            {
                "date_id": next_ds,
                "receipt_hospitals_active": df.loc["tablet", "hospitalId"],
                "receipt_requests_tablet": df.loc["tablet", "id"],
                "mobileReceipt_requests_app": df.loc["mobile", "id"],
                "mobileReceipt_requests_app_users": df.loc["mobile", "receiptPatientId"],
                "mobileReceipt_requests_app_completed": df.loc["mobile", "receiptStatus"],
                "mobileReceipt_hospitals_active": df.loc["mobile", "hospitalId"]
            }, index=[0]
        )
        return df_summary

class TreatmentsTransformer(Transformer):
    def transform(df, next_ds):
        df["status"] = df["status"].map(
            lambda x: 1 if x in (["completion", "paid", "paiderror", "orderprescription"]) else 0
        )
        df["productType"] = df["productType"].map(
            lambda x: "app" if x == "mobile" else "web"
        )
        df_1 = df.groupby("productType").agg(
            {
                "id": "size",
                "status": "sum",
                "patientId": "nunique",
                "doctorId": "nunique"
            }
        )
        try:
            df_1.loc["web", "id"]
        except KeyError:
            df_1.loc["web"] = {"id": 0, "status": 0, "patientId": 0, "doctorId": 0}
            pass

        df_summary = pd.DataFrame(
            {
                "date_id": next_ds,
                "untact_requests_web": df_1.loc["web", "id"],
                "untact_requests_web_users": df_1.loc["web", "patientId"],
                "untact_requests_app": df_1.loc["app", "id"],
                "untact_requests_app_users": df_1.loc["app", "patientId"],
                "untact_requests_completed": df_1["status"].sum(),
                "untact_doctors_active": df_1["doctorId"].sum(),
                "untact_doctors_completed": df[df.status==1]["doctorId"].nunique()
            }, index=[0]
        )

        return df_summary

class ReceiptHCTransformer(Transformer):
    def transform(df, next_ds):
        df_summary = pd.DataFrame(
            {
                "date_id": next_ds,
                "mobileReceipt_hospitals_available": df[df.mobileReceiptAvailable==1]["hospitalId"].nunique()
            }, index=[0]
        )
        return df_summary

class ReceiptSATransformer(Transformer):
    def transform(df, next_ds):
        df["installedAt"] = df["installedAt"].map(
            lambda x: datetime.strftime(
                datetime.strptime(x[:19], "%Y-%m-%d %H:%M:%S") + timedelta(hours=9), "%Y-%m-%d %H:%M:%S"
            )[:10] if x and str(x) != "nan" else np.nan 
        )
        df["withdrawalRequestedAt"] = df["withdrawalRequestedAt"].map(
            lambda x: datetime.strftime(
                datetime.strptime(x[:19], "%Y-%m-%d %H:%M:%S") + timedelta(hours=9), "%Y-%m-%d %H:%M:%S"
            )[:10] if x and str(x) != "nan" else np.nan 
        )
        df_summary = pd.DataFrame(
            {
                "date_id": next_ds, 
                "receipt_hospitals_install": df[df.status == "S3"]["hospitalId"].nunique(), 
                "receipt_hospitals_new": df[
                    (df.status == "S3") & (df.installedAt == next_ds)
                ]["hospitalId"].nunique(), 
                "receipt_hospitals_bounced": df[
                    (df.status == "W1") & (df.withdrawalRequestedAt == next_ds)
                ]["hospitalId"].nunique()
            }, index=[0]
        )
        return df_summary

class DailyKeyMetricsTransformer(Transformer):
    def transform(df):
        df.fillna(0, inplace=True)
        df["newInstall_total"] = (
            df["newInstall_organic_aos"]
            + df["newInstall_nonorganic_aos"]
            + df["newInstall_organic_ios"]
            + df["newInstall_nonorganic_ios"]
        )
        df["receipt_hospitals_activeRate"] = np.round(
            (df["receipt_hospitals_active"]/df["receipt_hospitals_install"]) * 100, 2
        )
        df["receipt_requests_total"] = (
            df["receipt_requests_tablet"] + df["receipt_requests_dummy"]
        )
        df["dau_web"] = df["dau_pc"] + df["dau_mobile"]
        df["dau_app"] = df["dau_aos"] + df["dau_ios"]
        df["mobileReceipt_requests_total"] = (
            0  # df["mobileReceipt_requests_web"] 
            + df["mobileReceipt_requests_app"]
        )
        df["mobileReceipt_requests_total_users"] = (
            0  # df["mobileReceipt_requests_web_users"] 
            + df["mobileReceipt_requests_app_users"]
        )
        df["mobileReceipt_requests_completed"] = (
            0  # df["mobileReceipt_requests_web_completed"]
            + df["mobileReceipt_requests_app_completed"]
        )
        df["mobileReceipt_requests_completedRate"] = np.round(
            (df["mobileReceipt_requests_completed"]/df["mobileReceipt_requests_total"]) * 100, 2
        )
        df["mobileReceipt_requests_avgPerHospitals"] = np.round(
            df["mobileReceipt_requests_total"]/df["mobileReceipt_hospitals_active"], 1
        )
        df["mobileAppointments_requests_total"] = (
            0  # df["mobileAppointments_requests_web"] 
            + df["mobileAppointments_requests_app"]
        )
        df["mobileAppointments_requests_total_users"] = (
            0  # df["mobileAppointments_requests_web_users"] 
            + df["mobileAppointments_requests_app_users"]
        )
        df["mobileAppointments_requests_completedRate"] = np.round(
            (df["mobileAppointments_requests_completed"]/df["mobileAppointments_requests_total"]) * 100, 2
        )
        df["untact_requests_total"] = (
            df["untact_requests_web"] 
            + df["untact_requests_app"]
        )
        df["untact_requests_total_users"] = (
            df["untact_requests_web_users"] 
            + df["untact_requests_app_users"]
        )
        df["untact_requests_completedRate"] = np.round(
            (df["untact_requests_completed"]/df["untact_requests_total"]) * 100, 2
        )
        df["keyEvents_requests"] = (
            df["mobileReceipt_requests_total"]
            + df["mobileAppointments_requests_total"]
            + df["untact_requests_total"]
        )
        df["keyEvents_requests_ofDAU"] = (
            np.round((df["keyEvents_requests"] / (df["dau_app"]+df["dau_web"])) * 100, 2)
        )
        df["keyEvents_requests_users"] = (
            df["mobileReceipt_requests_total_users"]
            + df["mobileAppointments_requests_total_users"]
            + df["untact_requests_total_users"]
        )
        df["keyEvents_requests_completed"] = (
            df["mobileReceipt_requests_completed"]
            + df["mobileAppointments_requests_completed"]
            + df["untact_requests_completed"]
        )
        df["keyEvents_requests_completedRate"] = np.round(
            (df["keyEvents_requests_completed"]/df["keyEvents_requests"]) * 100, 2
        )

        return df