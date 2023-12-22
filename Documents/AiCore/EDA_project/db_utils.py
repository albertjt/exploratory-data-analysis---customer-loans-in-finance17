# import yaml, pandas and create_engine
import yaml
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# create class that will be used to extract data from the Relational Database Service database
class RDSDatabaseConnector:

    # create a function to load the credentials.yaml file and returns the data dictionary contained within
    def load_credentials(self, file_path):
        """
        Load credentials from the specified YAML file.

        """
        with open(file_path, 'r') as file:
            credentials_data = yaml.safe_load(file)
        return credentials_data
            
    def init_sqlalchemy_engine(self, configuration_file_path):
        """
        Initialize a SQLAlchemy engine using the provided credentials.

        """
        credentials = self.load_credentials(configuration_file_path)
        engine = create_engine(f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")
        return engine.connect()
    
    def extract_loan_payments_data(self, engine):
        """
        Extract data from the 'loan_payments' table and return it as a Pandas DataFrame.

        """

        # Execute the query using Pandas read_sql_query and the initialized engine
        df = pd.read_sql_table(table_name="loan_payments", con=engine)
        return df
    
    def save_to_csv(self, df, file_path):
        """
        Save a Pandas DataFrame to a CSV file.

        """
        df.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")
    
    def load_data_to_dataframe(self, file_path):
        """
        Load data from a local CSV file into a Pandas DataFrame.

        """

        df = pd.read_csv(file_path)
        print(f"Data loaded from {file_path}")
        print(f"DataFrame shape: {df.shape}")
        return df


if __name__ == "__main__":
    db_conn = RDSDatabaseConnector()
    engine = db_conn.init_sqlalchemy_engine('credentials.yaml')
    print(engine)
    df = db_conn.extract_loan_payments_data(engine)
    print(df)
#    df = db_conn.table_to_dataframe("loan_payments")
#    df.to_csv("loan_payments.csv")

  