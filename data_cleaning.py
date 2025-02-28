from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import numpy as np
import pandas as pd
import re

class DataCleaning:
    '''
    This class will work as a utility class, in it you will be creating methods that 
    help extract data from different data sources.

    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.

    Attributes:
        None
    '''

    # def __init__(self, param_1, param_2 = 5):
    #     pass

    def clean_user_data(self, user_data_df: pd.DataFrame) -> pd.DataFrame:
        '''
        This method will perform the cleaning of the user data.

        Args:
            user_data_df(DataFrame): DataFrame containing raw user details

        Returns:
            df_with_valid_dates(DataFrame): cleaned DataFrame containing valid user details
        '''
        
        # Replace all 'NULL' values in dataframe with a NaN
        user_data_df.replace('NULL', np.nan, inplace=True)

        # Drop rows that contain mostly missing values (less than 2 non-NaN values)
        df_dropped = user_data_df.dropna(thresh=len(user_data_df.columns)-1)

        # Performing a deep copy to avoid wrong referencing issues
        df_dropped = df_dropped.copy()

        # Change the data type of the join_date and date_of_birth columns to datetime type
        df_dropped['join_date'] = pd.to_datetime(df_dropped['join_date'], format="mixed", errors='coerce')
        df_dropped['date_of_birth'] = pd.to_datetime(df_dropped['date_of_birth'], format="mixed", errors='coerce')

        #df_dropped = self.change_join_date_type(df_dropped) # This method is not used for now

        # Drop rows that contain NaT values in either of the 2 datetime columns.
        # The analysis shows that they contain invalid data in the other columns except for the index column.
        df_with_valid_dates = df_dropped.dropna(subset=['date_of_birth', 'join_date'])
        print(f"The cleaned DataFrame has {len(df_with_valid_dates)} rows")
        return df_with_valid_dates

    def change_join_date_type(self, user_data_df):
        '''
        This method will perform the cleaning of the user data.

        Args:
            user_data_df(DataFrame): DataFrame containing...

        Returns:
            None (TBD)
        '''
        user_data_df['join_date'] = pd.to_datetime(user_data_df['join_date'], format="mixed", errors='coerce')
        
        return user_data_df
        
    def clean_card_data(self, card_data_df: pd.DataFrame) -> pd.DataFrame:
        '''
        This method will clean the data to remove any erroneous values, NULL values or errors with formatting.

        Args:
            card_data_df(DataFrame): DataFrame containing raw card details

        Returns:
            clean_df(DataFrame): cleaned DataFrame containing valid card details
        '''
        # Drop rows that contain table header information
        card_data_df.drop(card_data_df.loc[card_data_df['card_number'] == 'card_number'].index, inplace=True)
        print(f"Length of df after cleaning phase 1: {len(card_data_df)}")

        # Drop rows that do not have at least 3 non-NA values
        card_data_df.dropna(thresh=3, inplace=True)
        print(f"Length of df after cleaning phase 2: {len(card_data_df)}")

        # Drop rows that do not match the MM/YY regex for expiry date
        regex = r'^(0[1-9]|1[0-2])\/\d{2}$'
        card_data_df.drop(
            card_data_df.loc[~card_data_df['expiry_date'].str.match(regex, na=False)].index, inplace=True
            )
        print(f"Length of df after cleaning phase 3: {len(card_data_df)}")

        # Performing a deep copy to avoid wrong referencing issues
        clean_df = card_data_df.copy()

        # Change the data type of the date_payment_confirmed column to datetime type
        clean_df['date_payment_confirmed'] = pd.to_datetime(clean_df['date_payment_confirmed'], 
                                                            format="mixed", errors='coerce')
        
        print(f"The cleaned DataFrame has {len(clean_df)} rows")
        return clean_df
    
    def clean_store_data(self, stores_data_df: pd.DataFrame)  -> pd.DataFrame:
        '''
        This method will clean the data retrieved from the API and returns a pandas DataFrame.

        Args:
            stores_data_df(DataFrame): DataFrame containing details of stores in the business

        Returns:
            clean_df(DataFrame): cleaned DataFrame
        '''

        # Replace all 'NULL' values in dataframe with a NaN
        stores_data_df.replace('NULL', np.nan, inplace=True)

        # Drop rows that do not have at least 8 non-NA values
        stores_data_df.dropna(thresh=8, inplace=True)
        print(f"Length of df after cleaning phase 1: {len(stores_data_df)}")

        # Change the data type of the opening_date column to datetime type
        stores_data_df['opening_date'] = pd.to_datetime(stores_data_df['opening_date'], 
                                                                    format="mixed", errors='coerce')

        # Drop rows with random faulty values i.e. D23PCWSM6S
        stores_data_df.dropna(subset=["opening_date"], inplace=True)
        print(f"Length of df after cleaning phase 2: {len(stores_data_df)}")

        # Removing leading and trailing whitespaces from values in the staff_numbers column
        stores_data_df['staff_numbers'] = stores_data_df['staff_numbers'].str.strip()
        # Removing whitespaces between values in the staff_numbers column
        stores_data_df['staff_numbers'] = stores_data_df['staff_numbers'].str.replace(r'\s+', '', regex=True)
        # Removing all Letters and Symbols (Keep Only Numbers)
        stores_data_df['staff_numbers'] = stores_data_df['staff_numbers'].str.replace(r'[^0-9\s]', '', regex=True)

        clean_df = stores_data_df.copy()

        print(f"The cleaned DataFrame has {len(clean_df)} rows")
        return clean_df
    
    def convert_product_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts the weight column in the products DataFrame to kg.
        
        Parameters:
            df (pd.DataFrame): The products DataFrame.
        
        Returns:
            df(pd.DataFrame): The updated DataFrame with weight values in kg.
        """
        
        def convert_weight(value):
            value = str(value).lower().strip()

            # Handle "12 x 100g" cases
            match = re.match(r"(\d+)\s*x\s*(\d+)(g|kg|ml)", value)
            if match:
                count, unit_value, unit = match.groups()
                total_value = int(count) * int(unit_value)
                value = f"{total_value}{unit}"

            # Extract numeric value and unit
            match = re.match(r"([\d.]+)\s*(kg|g|ml)", value)
            if match:
                num, unit = match.groups()
                num = float(num)

                # Convert to kg
                if unit == "kg":
                    return num
                elif unit in ["g", "ml"]:
                    return num / 1000
            
            return None

        df["weight"] = df["weight"].apply(convert_weight)
        return df
    
    def clean_products_data(self, products_data_df: pd.DataFrame) -> pd.DataFrame:
        '''
        This method will clean the products data DataFrame of any additional erroneous values.

        Args:
            products_data_df(DataFrame): DataFrame containing details of products in the company

        Returns:
            clean_df(DataFrame): cleaned DataFrame
        '''

        # Replace all 'NULL' values in dataframe with a NaN
        products_data_df.replace('NULL', np.nan, inplace=True)

        # Drop rows that do not have at least 7 non-NA values
        products_data_df.dropna(thresh=7, inplace=True)
        print(f"Length of df after cleaning phase 1: {len(products_data_df)}")

        # Change the data type of the date_added column to datetime type
        products_data_df['date_added'] = pd.to_datetime(products_data_df['date_added'], 
                                                                        format="mixed", errors='coerce')

        # Drop rows with random faulty values i.e. CCAVRB79VV
        products_data_df.dropna(subset=["date_added"], inplace=True)
        print(f"Length of df after cleaning phase 2: {len(products_data_df)}")

        clean_df = products_data_df.copy()

        print(f"The cleaned DataFrame has {len(clean_df)} rows")
        return clean_df
    
    def clean_orders_data(self, orders_data_df: pd.DataFrame) -> pd.DataFrame:
        '''
        This method will clean the orders table data (that is represented by a DataFrame) of any additional erroneous values.

        Args:
            orders_data_df(DataFrame): DataFrame containing details of sale orders in the company

        Returns:
            clean_df(DataFrame): cleaned DataFrame
        '''

        # Remove the columns, first_name, last_name and 1 to have the table in the correct form before uploading to the database.
        print(f"Length of df columns before cleaning: {len(orders_data_df.columns)}")
        orders_data_df.drop('first_name', axis=1, inplace=True)
        orders_data_df.drop('last_name', axis=1, inplace=True)
        orders_data_df.drop('1', axis=1, inplace=True)
        print(f"Length of df columns after cleaning: {len(orders_data_df.columns)}")

        clean_df = orders_data_df.copy()

        print(f"The cleaned DataFrame has {len(clean_df)} rows and {len(orders_data_df.columns)} columns")
        return clean_df
    
    def clean_dates_data(self, dates_data_df: pd.DataFrame) -> pd.DataFrame:
        '''
        This method will clean the products data DataFrame of any additional erroneous values.

        Args:
            dates_data_df(DataFrame): DataFrame containing details of sale date details

        Returns:
            clean_df(DataFrame): cleaned DataFrame
        '''

        # Replace all 'NULL' values in dataframe with a NaN
        dates_data_df.replace('NULL', np.nan, inplace=True)

        # Convert values in columns "day", "month", and "year" into numeric values
        dates_data_df['day'] = pd.to_numeric(dates_data_df.day, errors='coerce')
        dates_data_df['month'] = pd.to_numeric(dates_data_df.month, errors='coerce')
        dates_data_df['year'] = pd.to_numeric(dates_data_df.year, errors='coerce')

        # Drop rows that do not have at least 4 non-NA values
        dates_data_df.dropna(thresh=4, inplace=True)
        print(f"Length of df after cleaning phase 1: {len(dates_data_df)}")

        # Convert values in columns "day", "month", and "year" into integer values
        # The previous conversion wouldn't achieve that because of the presence of NaNs
        dates_data_df['day'] = pd.to_numeric(dates_data_df.day, errors='coerce', downcast='integer')
        dates_data_df['month'] = pd.to_numeric(dates_data_df.month, errors='coerce', downcast='integer')
        dates_data_df['year'] = pd.to_numeric(dates_data_df.year, errors='coerce', downcast='integer')

        clean_df = dates_data_df.copy()

        print(f"The cleaned DataFrame has {len(clean_df)} rows")
        return clean_df

def main():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()
    
    # Execute Users Data Upload - RUN ONCE
    table_name = 'legacy_users'
    user_data_df = data_extractor.read_rds_table(db_connector, table_name)
    df = data_cleaner.clean_user_data(user_data_df)
    db_connector.upload_to_db(df, 'dim_users')

    # Execute Card Data Upload - RUN ONCE
    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_data_df = data_extractor.retrieve_pdf_data(link)
    pdf_data_length = len(data_extractor.retrieve_pdf_data(link))
    print(f"The table in the PDF has {pdf_data_length} rows")
    card_df = data_cleaner.clean_card_data(card_data_df)
    print(f"The card data dataframe has {len(card_df)} rows")
    print(card_df.dtypes)
    db_connector.upload_to_db(card_df, 'dim_card_details')

    # Execute Store Data Upload - RUN ONCE
    endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    stores_data = data_extractor.retrieve_stores_data(endpoint)
    stores_data_length = len(stores_data)
    print(f"The table in the Stores DF has {stores_data_length} rows")
    stores_df = data_cleaner.clean_store_data(stores_data)
    print(f"The stores dataframe has {len(stores_df)} rows")
    print(stores_df.dtypes)
    db_connector.upload_to_db(stores_df, 'dim_store_details')

    # Execute Products Data Upload - RUN ONCE
    s3_uri = "s3://data-handling-public/products.csv"
    products_data = data_extractor.extract_from_s3(s3_uri)
    products_data_length = len(products_data)
    print(f"The table in the Products DF has {products_data_length} rows")
    products_df = data_cleaner.convert_product_weights(products_data)
    products_df = data_cleaner.clean_products_data(products_df)
    print(f"The products dataframe has {len(products_df)} rows")
    print(products_df.dtypes)
    db_connector.upload_to_db(products_df, 'dim_products')

    # Execute Orders Data Upload - RUN ONCE
    table_name = 'orders_table'
    orders_data_df = data_extractor.read_rds_table(db_connector, table_name)
    clean_orders_df = data_cleaner.clean_orders_data(orders_data_df)
    db_connector.upload_to_db(clean_orders_df, 'orders_table')

    # Execute Date Details Data Upload - RUN ONCE
    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    json_data = data_extractor.retrieve_json_data(link) 
    json_data_length = len(json_data) 
    print(f"The table in the original JSON has {json_data_length} rows")
    dates_df = data_cleaner.clean_dates_data(json_data)
    print(f"The date details dataframe has {len(dates_df)} rows")
    print(dates_df.dtypes)
    db_connector.upload_to_db(dates_df, 'dim_date_times')

if __name__ == "__main__":
    main()