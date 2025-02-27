import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pprint

class DatabaseConnector:
    '''
    This class will work as a utility class, in it you will be creating methods that 
    help extract data from different data sources.

    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.

    Attributes: ***TO DO***
        word(str): The word to be guessed, picked randomly from the word_list.
        word_guessed (list): A list of the letters of the word, with _ for each letter not yet guessed.
        list_of_guesses(list): A list of the guesses that have already been tried.
    '''

    def __init__(self):
        pass

    def read_db_creds(self, remote=True):
        '''
        This method will read the credentials yaml file and return a dictionary of the credentials.

        Args:
            remote(bool): Is the database remote or local?

        Returns:
            config(dict)
        '''
        db_file_name = 'db_creds.yaml' if remote else 'local_db_creds.yaml'
        with open(db_file_name, 'r') as file:
            config = yaml.safe_load(file)

        return config
    
    def init_db_engine(self, remote=True):
        '''
        This method will read the credentials from the return of read_db_creds and initialise and 
        return an sqlalchemy database engine.

        Args:
            remote(bool): Is the database remote or local?

        Returns:
            engine(Engine)
        '''
        db_cred = self.read_db_creds(remote)

        # initialise and return an sqlalchemy database engine
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = db_cred['RDS_HOST'].strip()
        USER = db_cred['RDS_USER']
        PASSWORD = db_cred['RDS_PASSWORD']
        DATABASE = db_cred['RDS_DATABASE']
        PORT = db_cred['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", isolation_level='AUTOCOMMIT')
        return engine

    def list_db_tables(self):
        '''
        This method will to list all the tables in the database so you know which tables you 
        can extract data from.

        Args:
            None

        Returns:
            None
        '''
        engine = self.init_db_engine()

        #engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        
        inspector = inspect(engine)
        db_tables = inspector.get_table_names()
        pprint.pprint(db_tables)
        

    def upload_to_db(self, df, table_name):
        '''
        This method will take in a Pandas DataFrame and table name to upload to as an argument.

        Args:
            df(DataFrame): DataFrame to be uploaded to database
            table_name(str): Name of table to be uploaded

        Returns:
            None
        '''
        engine = self.init_db_engine(remote=False)

        # engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        print(engine.url)

        # Upload the DataFrame to the database table
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
        inspector = inspect(engine)
        db_tables = inspector.get_table_names()
        pprint.pprint(db_tables)

def main():
    db_connector = DatabaseConnector()
    #engine = db_connector.init_db_engine()
    #engine.execution_options(isolation_level='AUTOCOMMIT').connect()
    db_connector.list_db_tables()
    

    #print(type(db_cred))
    #print(db_cred)
    

if __name__ == "__main__":
    main()