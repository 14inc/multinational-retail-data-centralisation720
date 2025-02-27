# import random

from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import text
import tabula
import requests
import boto3
from io import StringIO

class DataExtractor:
    '''
    This class will work as a utility class, in it you will be creating methods that 
    help extract data from different data sources.

    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.

    Attributes:
        word(str): The word to be guessed, picked randomly from the word_list.
        word_guessed (list): A list of the letters of the word, with _ for each letter not yet guessed.
        list_of_guesses(list): A list of the guesses that have already been tried.
    '''

    # def __init__(self, word_list, num_lives = 5):
    #     #self.word = random.choice(word_list)
    #     #self.word_guessed = list('_' * len(self.word))
    #     #self.list_of_guesses = []

    def read_rds_table(self, db_connector, table_name):
        '''
        This method will extract the database table to a pandas DataFrame.

        Args:
            db_connector(DatabaseConnector): an instance of your DatabaseConnector class
            table_name(str): Name of table in AWS RDS database

        Returns:
            df(DataFrame): pandas DataFrame containing database table data
        '''

        try:
            engine = db_connector.init_db_engine()
            #engine.execution_options(isolation_level='AUTOCOMMIT').connect()

            df = pd.read_sql_table(table_name, engine)

            # with engine.connect() as connection:
            #     result = connection.execute(text(f"SELECT * FROM {table_name}"))
            #     print(type(result))
            #     #print(result[0])
            #     for row in result:
            #         print(row)
            #         break
        except Exception as e:
            print("Exception thrown while reading SQL table from database:", e)

        return df
    
    def retrieve_pdf_data(self, link):
        '''
        This method will take in a link to a PDF containing a table as an argument and returns a pandas DataFrame.

        Args:
            link(str): URL of remote PDF file

        Returns:
            tabula_df(DataFrame): pandas DataFrame containing table extracted from remote PDF file
        '''

        try:
            # Read remote pdf into list of DataFrame
            tabula_list = tabula.read_pdf(link, multiple_tables=False, pages='all')
            tabula_df = tabula_list[0]
        except Exception as e:
            print("Exception thrown while reading table from remote PDF:", e)

        return tabula_df
    
    def list_number_of_stores(self, number_of_stores_endpoint, header_dict):
        '''
        This method will return the number of stores to extract.

        Args:
            number_of_stores_endpoint(str): number of stores endpoint
            header_dict(dict): a dictionary containing the header details including x-api-key

        Returns:
            number_of_stores(int): number of stores (in the business) to extract
        '''

        try:
            # Calling API
            response = requests.get(number_of_stores_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                number_of_stores = data['number_stores']
            else:
                number_of_stores = None
        except Exception as e:
            print("Exception thrown while calling API to return the number of stores:", e)

        return number_of_stores
    
    def retrieve_stores_data(self, retrieve_a_store_endpoint):
        '''
        This method will extract all the stores from the API saving them in a pandas DataFrame.

        Args:
            number_of_stores_endpoint(str): number of stores endpoint
            header_dict(dict): a dictionary containing the header details including x-api-key

        Returns:
            stores_df(DataFrame): DataFrame containing data for all stores in the business
        '''

        number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        header_dict = {
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }

        number_of_stores = self.list_number_of_stores(number_of_stores_endpoint, header_dict)

        stores_df = pd.DataFrame()
        for store_number in range(number_of_stores):
            url = f"{retrieve_a_store_endpoint}/{store_number}"
            response = requests.get(url, headers=header_dict)
            
            if response.status_code == 200:
                record = response.json()
                stores_df = pd.concat([stores_df, pd.DataFrame([record])], ignore_index=True)

        return stores_df
    
    def extract_from_s3(self, s3_uri: str) -> pd.DataFrame:
        """
        Downloads a CSV file from an S3 bucket and returns it as a pandas DataFrame.
        
        Parameters:
            s3_uri (str): The S3 address of the file in the format 's3://bucket-name/file.csv'
        
        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        # Parse the S3 URI
        if not s3_uri.startswith("s3://"):
            raise ValueError("Invalid S3 URI. It should start with 's3://'")
        
        s3_parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket_name, object_key = s3_parts[0], s3_parts[1]
        
        # Initialize the S3 client
        s3 = boto3.client('s3')
        
        # Download the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_data = response['Body'].read().decode('utf-8')
        #print(type(csv_data)) <class 'str'>
        
        # Convert to pandas DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        return df
    
    def retrieve_json_data(self, link: str) -> pd.DataFrame:
        '''
        This method will take in a link to a JSON in S3 containing a table as an argument and 
        returns a pandas DataFrame.

        Args:
            link(str): URL of remote JSON file

        Returns:
            json_df(DataFrame): pandas DataFrame containing table extracted from remote JSON file
        '''

        try:
            # Read remote json into a DataFrame
            json_df = pd.read_json(link)
        except Exception as e:
            print("Exception thrown while reading data from remote json file:", e)

        return json_df

def main():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    table_name = 'legacy_users'
    user_data_df = data_extractor.read_rds_table(db_connector, table_name)
    print(user_data_df.head(3))
    print(f"{len(user_data_df)} rows in legacy users dataframe")

    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    pdf_data_length = len(data_extractor.retrieve_pdf_data(link))
    print(f"The table in the PDF has {pdf_data_length} rows")

    endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    stores_data_length = len(data_extractor.retrieve_stores_data(endpoint))
    print(f"The table in the Stores DF has {stores_data_length} rows")

    s3_uri = "s3://data-handling-public/products.csv"
    products_data_length = len(data_extractor.extract_from_s3(s3_uri))
    print(f"The table in the Products DF has {products_data_length} rows")

    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    json_data = data_extractor.retrieve_json_data(link) 
    json_data_length = len(json_data) 
    print(f"The table in the JSON has {json_data_length} rows")

if __name__ == "__main__":
    main()
    # Move code in main() here to avoid it being called as part of the namespace when module is imported.