import drive_test as drive
import dataresource as collector
import database_handler as db

# collector.get_saved_versions("Italy-Collection-2020", "it-it")
table_cols = db.get_columns(database="search_automation",
                            table="text_data_reserve")
print(table_cols)
