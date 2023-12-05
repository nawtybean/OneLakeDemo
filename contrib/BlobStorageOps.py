import time
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from azure.storage.blob._generated.models import CopyStatusType

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_rows", None)


class BlobStorage:
    """
    Author: Shaun De Ponte

    This is a Blob Class - Get a list of files from a blob and do somethings
    with them

    Attributes
    ----------
        BLOB_ACCOUNT_NAME:
            This is the account name of the blob
        BLOB_PRIMARY_KEY:
            The primary key used to connect to the blob
        service_client:
            Initialize variable to use later on
    """

    def __init__(self, BLOB_ACCOUNT_NAME, BLOB_PRIMARY_KEY, BLOB_CONNECTION_STRING):
        """
        Inititalize and assign variables
        """
        self.BLOB_ACCOUNT_NAME = BLOB_ACCOUNT_NAME
        self.BLOB_PRIMARY_KEY = BLOB_PRIMARY_KEY
        self.BLOB_CONNECTION_STRING = BLOB_CONNECTION_STRING
        self.service_client = ''

    def connect_lake_blob(self):
        """
        Connect to the Blob using DataLake Service.
    
        Returns
        -------
        None
        """
        try:  
            self.service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", self.BLOB_ACCOUNT_NAME), credential=self.BLOB_PRIMARY_KEY)

            print('Connecting to Blob Data Lake Successful')
        except Exception as e:
            print(f"Something went wrong: {e}")

    def connect_service_blob(self):
        """
        Connect to the Blob using Client Service.
    
        Returns
        -------
        Type:
            object
        Data:
            blob_service_client
        """
        try:  
            self.blob_service_client = BlobServiceClient.from_connection_string(self.BLOB_CONNECTION_STRING)
            print('Connecting to Blob Service Client Successful')
            return self.blob_service_client
        
        except Exception as e:
            print(f"Something went wrong: {e}")

    def connect_container(self, container:str):
        self.file_system_client = self.service_client.get_file_system_client(file_system=container)

    def list_directory_contents(self, container, folder):
        """
        Get a list of files in a blob.
    
        Returns
        -------
        Type:
            List
        Data:
            ['folderName/file1.csv',
             'folderName/file2.csv']
        """
        try:
            # Connect to the storage account.
            self.connect_lake_blob()
            # Connect to the contianer.
            self.file_system_client = self.service_client.get_file_system_client(file_system=container)
            # Retrieve the contents in the directory.
            self.paths = self.file_system_client.get_paths(path=folder)
            # Create a list and append each file and folder in the directory.
            ls = []
            for path in self.paths:
                #print(path)
                ls.append(path.name)

            return ls
        # An error was encountered while trying to retrtieve the 
        # directory contents.
        except Exception as e:
            print(f"Something went wrong: {e}")
                     
    def connection_url(self, container):
        """
        Constructs a connection url. This is the second step in 
        setting the spark context
    
        Returns
        -------
        Type:
            String
        Data:
            "abfss://<container_name>@<account_name>.dfs.core.windows.net"
        """
        try:
            url = "abfss://" + container + "@" + self.BLOB_ACCOUNT_NAME + ".dfs.core.windows.net"
            print('Creating connection url successful')
            return url
        except Exception as e:
            print(f"Something went wrong: {e}")
        
    def check_blob_exists(self, container, blob):
        """
        Checks to see if the blob exists

        Returns
        -------
        Type:
            Boolean
        Data:
            True / False
        """
        try:
            blob = BlobClient.from_connection_string(conn_str=self.BLOB_CONNECTION_STRING, container_name=container, blob_name=blob)
            exists = blob.exists()
            
            return exists
        except Exception as e:
            print(f"Something went wrong: {e}")
 
    def create_container_if_not_exists(self, container):
        """
        Checks to see if the container exists, if not create it

        Returns
        -------
        None
        """
        container = ContainerClient.from_connection_string(conn_str=self.BLOB_CONNECTION_STRING, container_name=container)
        try:
            container_properties = container.get_container_properties()
        except Exception as e:
            container.create_container()     

    def az_copy(self, container_name, file_path_on_azure, local_path):    
        """
        Copy from the DBFS store in DataBricks and upload to the Azure Blob Storage

        Returns
        -------
        None
        """
        service_client = BlobServiceClient.from_connection_string(self.BLOB_CONNECTION_STRING)
        container_client = service_client.get_container_client(container_name)  

        blob_client = container_client.get_blob_client(file_path_on_azure)

        with open(local_path, 'rb') as data:
            print(data)
            blob_client.upload_blob(data, blob_type="BlockBlob")


    def download_blob(self, container: str, blob_path: str):
        """Download a blob from the storage account and return the contents in bytes format.

        Args:
            container (str): Storage account container that blob can be found in.
            blob_path (str): Relative path to blob inside container.

        Returns:
            Bytes contents of blob.
        """
        blob_service_client = BlobServiceClient.from_connection_string(self.BLOB_CONNECTION_STRING)
        # Create a connection to desired container.
        container_client = blob_service_client.get_container_client(container)
        # Download and read the blob.
        blob = container_client.download_blob(blob_path).readall()
        return blob


    def move_or_copy_blob(self, source_container: str, source_dir: str, target_container: str, target_dir: str, remove_target=False, copy=False):
        """Move  blob from one path in a storage account cotainer to another path in the same storage account.
        If remove_target is set to True, the target blob will be deleted if it already exists.

        Args:
            source_container (str): Storage account container that blob can be found in.
            source_dir (str): Relative path to where blob is currently located.
            target_container (str): Target container.
            target_dir (str): Target relative path for the blob.
            remove_target (bool): Indicates if the target blob must be removed first if it exists.
            copy (bool): Indicates if the source blob must be deleted after the copy has been performed.

        Returns:
            None
        """
        blob_service_client = BlobServiceClient.from_connection_string(self.BLOB_CONNECTION_STRING)
        # Create a client for the source blob.
        source_container_client = blob_service_client.get_container_client(source_container)
        source_blob = source_container_client.get_blob_client(source_dir)
        # Identify the destination blob.
        target_container_client = blob_service_client.get_container_client(target_container)
        destination_blob = target_container_client.get_blob_client(target_dir)
        # Delete the target blob if specified and it already exists.
        if remove_target == True:
            # If the target blob exists.
            if destination_blob.exists() == True:
                # Remove the target blob.
                destination_blob.delete_blob()
        # Begin the copy operation
        destination_blob.start_copy_from_url(source_url=source_blob.url)
        
        def _get_destination_file_copy_status(destination_blob: BlobClient) -> str:
            # Get the destination blob properties
            destination_blob_properties = destination_blob.get_blob_properties()
            return destination_blob_properties.copy.status
        
        # Get the copy status.
        copy_status = _get_destination_file_copy_status(destination_blob)
        # Wait for the blob to finish copying.
        timeout = 30 # In seconds.
        copy_time_elapsed = 0
        wait_period = 2 # In seconds
        if copy_status != CopyStatusType.success:
            while (copy_status != CopyStatusType.success) & (copy_time_elapsed < timeout):
                time.sleep(wait_period)
                # Get the copy status.
                copy_status = _get_destination_file_copy_status(destination_blob)
                copy_time_elapsed = copy_time_elapsed + wait_period
                if copy_time_elapsed >= timeout:
                    raise Exception(f"Timeout {timeout}seconds exceeded moving file. Source dir: {source_dir}, Target dir: {target_dir}")
        # Delete the source file if this is a move and not a copy.
        if copy == False:
            source_blob.delete_blob()

    def delete_blob(self, container: str, blob_dir: str):
        """Delete a blob from the storage account.
        
        Args:
            container (str): Storage account container that blob can be found in.
            blob_dir (str): Relative path to where blob that must be deleted.
            
        Returns:
            None

        """
        blob_service_client = BlobServiceClient.from_connection_string(self.BLOB_CONNECTION_STRING)
        # Create a client for blob
        blob = blob_service_client.get_blob_client(container=container, blob=blob_dir)
        # Remove the target blob.
        blob.delete_blob()

    def delete_empty_folders(self, container: str, root_folder: str):
        """Delete any empty folders found in a directory. This is done by navigating from
        the lowest to highest level specified by the root directory and removing any folders
        that do not contain any files.

        Args:
            container (str): Conteiner to perform the operation in.
            root_folder (str): Path of the root folder to identify and remove
                empty folders in.
        """
        # Connect to the storage account.
        self.connect_lake_blob()
        # Connect to the contianer.
        self.connect_container(container)
        # Retrieve the contents in the directory.
        paths = self.file_system_client.get_paths(path=root_folder)
        contents = []
        # Fetch the size and slit the path by "/", this will be used to determine
        # the directory structure.
        for path in paths:
            contents.append({
                "Path": path.name,
                "SplitPath": path.name.split("/"),
                "ContentLength": path.content_length
            })
        # Determine the max depth of sub folders in the directory.
        max_depth = 0
        for item in contents:
            if len(item["SplitPath"]) > max_depth:
                max_depth = len(item["SplitPath"])
        # Create a dataframe containing the hierarchy of the directory.
        for i in range(0, len(contents)):
            split_path = contents[i]["SplitPath"]
            levels = {}
            for j in range(0, max_depth):
                if j < len(split_path):
                    levels[f"L{str(j)}"] = split_path[j]
                else:
                    levels[f"L{str(j)}"] = None
            contents[i] = {**contents[i], **levels}
        # Create the directory hierarchy of the contents.
        df = pd.DataFrame(data=contents)
        sort_values = [f"L{str(i)}" for i in range(0, max_depth)]
        df = df.sort_values(sort_values)
        # Iterate through the contents from lowest to highest level and
        # determine which are empty folders.
        df["EmptyFolder"] = False
        # For loop from lowest to highest level.
        for level in range(max_depth-1, -1, -1):
            # Filter out any other folders that have already been marks as empty.
            temp_df = df[df["EmptyFolder"] == 0]
            # Create a list of unique not null values for the level.
            unique_values = temp_df[f"L{str(level)}"].unique().tolist()
            if None in unique_values:
                unique_values.remove(None)
            # Iterate through each item for the level.
            for item in unique_values:
                # Determine the row count, ie: how many items are in the level.
                row_count = len(temp_df[temp_df[f"L{str(level)}"] == item])
                # Determien the max size of an obejct for the level, ie: if not zero the item
                # is a file or contains a file.
                max_content_length = temp_df["ContentLength"][temp_df[f"L{str(level)}"] == item].max()
                # If only one item is found in the level and it is not a file.
                if (row_count == 1) & (max_content_length == 0):
                    # Mark the level as an empty folder.
                    df["EmptyFolder"][
                        df[f"L{str(level)}"] == item
                    ] = True
        # Remove the empty folders, start from lowest sub level, to highest.
        df = df[df["EmptyFolder"] == True]
        df = df.reset_index(drop=True)
        df["IsDeleted"] = False
        for level in range(max_depth-1, -1, -1):
            for i in range(0, len(df)):
                path = df.loc[i, "Path"]
                # If the value is not null and the row hasn't already been deleted.
                if (df.loc[i, f"L{str(level)}"] is not None) &\
                    (df.loc[i, "IsDeleted"] == False):
                    # Add a deleted indicator to the dataframe, so that
                    # a delete is not attempted on the same record again.
                    df.loc[i, "IsDeleted"] = True
                    # Delete the folder.
                    self.delete_blob(container, path)

    def get_size_of_directory(self, container: str, directory: str, output_size="GB") -> float:
        # Connect to the storage account.
        self.connect_lake_blob()
        # Connect to the contianer.
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        # Retrieve the contents in the directory.
        paths = file_system_client.get_paths(path=directory)
        # Create a list and append each files size to the list.
        ls = []
        for path in paths:
            ls.append(path.content_length)
        # Calculate the total size of the folder in the desired size, ie: GB or MB.
        total_size_bytes = sum(ls)
        if output_size == "GB":
            output_size = round(total_size_bytes / (1024**3), 2)
        elif output_size == "GB":
            output_size = round(total_size_bytes / (1024**2), 2)
        else:
            raise Exception(f"Output size type {str(output_size)} not supported.")
        return output_size

    @staticmethod
    def split_path_to_parts(path: str) -> tuple:
        """Splits an absolute path into it constituent parts.
        ie: abfss://name@devcontainer.dfs.core.windows.net/Processed_Successful/name/
        becomes
        container = "name"
        directory = "Processed_Successful/name/"
        url = "abfss://name@devcontainer.dfs.core.windows.net/"

        Args:
            path (str): Absolute path to folder or file in storage account.

        Returns:
            (container, directory, url)
        """
        # Fetch the part of the path containing the container name.
        container_part = path.split("@", 1)[0]
        # Isolate the container name.
        container = container_part.split("//", 1)[1]
        # Fetch the part of the path containing the directory.
        directory = path.split("windows.net/", 1)[1]
        # Isolate the storage account url.
        url = path.split("windows.net/", 1)[0]
        url = f"{url}windows.net/"
        return (container, directory, url)
