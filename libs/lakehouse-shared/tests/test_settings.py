from lakehouse_shared import IcebergCatalogSettings


def test_iceberg_identifier_combines_database_and_table():
    settings = IcebergCatalogSettings(
        glue_database="pms_lakehouse", iceberg_table_name="price_decision_raw"
    )

    assert settings.iceberg_identifier == "pms_lakehouse.price_decision_raw"
