from typing import Tuple

from process.util.data_transfer import RetailerDatabaseContext


class PrtSalesCalculator(object):
    @staticmethod
    def scg_to_org_code_user_no_kv(pair: Tuple[RetailerDatabaseContext, dict]):
        retailer_database_context = pair[0]
        data = pair[1]
        return (retailer_database_context.org_code, data["user_no"]), data

    @staticmethod
    def to_org_code_user_no_kv(pair: Tuple[RetailerDatabaseContext, dict]):
        retailer_database_context = pair[0]
        data = pair[1]
        return (retailer_database_context.org_code, data["sale_guider_code"]), data