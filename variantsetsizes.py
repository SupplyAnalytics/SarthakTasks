from __future__ import with_statement
from AnalyticsClient import AnalyticsClient
import pandas as pd
import time

def create_jobid():
    class Config:
        CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K"
        CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c"
        REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce"
        ORGID = "60006357703"
        WORKSPACEID = "174857000004732522"
        VIEWID = "174857000101980337"

    class sample:
        ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN)      
        def initiate_bulk_export(self, ac):
            response_format = "csv"
            bulk = ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)
            result = bulk.initiate_bulk_export(Config.VIEWID, response_format) 
            print(result)
            return result
    try:
        obj = sample()
        var1 = obj.initiate_bulk_export(obj.ac) 
    except Exception as e:
        print(str(e))

    return var1  

def get_jobid(job_id):  
    class Config:
        CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K"
        CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c"
        REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce"
        ORGID = "60006357703"
        WORKSPACEID = "174857000004732522"

    class sample:
        ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN)

        def get_export_job_details(self, ac):
            bulk = ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)
            result = bulk.get_export_job_details(job_id)  
            print(result)
            return result
    try:
        obj = sample()
        obj.get_export_job_details(obj.ac)
        return obj.get_export_job_details(obj.ac)

    except Exception as e:
        print(str(e))
        return None

def export(job_id):
    class Config:

        CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K"
        CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c"
        REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce"
        ORGID = "60006357703"
        WORKSPACEID = "174857000004732522"
        VIEWID = "174857000101980337"

    class sample:

        ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN)

        def export_bulk_data(self, ac):
            file_path = 'VariantSizeText.csv'
            bulk = ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)
            bulk.export_bulk_data(job_id, file_path)

    try:
        obj = sample()
        obj.export_bulk_data(obj.ac);

    except Exception as e:
        print(str(e))


def create_jobid_loop():
    while True:
        job_id = create_jobid()
        if job_id is not None:
            print("Job ID:", job_id)
            break
        else:
            print("Job ID creation failed. Retrying in 5 seconds...")
            time.sleep(30)

    return job_id

def get_jobid_loop(job_id):
    while True:
        result = get_jobid(job_id)
        print("Result:", result)  # Print the result for debugging
        if result is not None and 'jobCode' in result and result['jobCode'] == '1004':
            print("Job ID:", job_id, "Status Code:", result['jobCode'])
            break
        else:
            print("Retrieving job status failed. Retrying in 30 seconds...")
            time.sleep(30)

    return result['jobCode'] if result is not None else None

def extract_unique_sizes(df):
    # Split the sizetext column by commas to handle multiple sizes
    sizes_list = df['sizetext'].str.split(',')
    
    # Drop rows where sizes_list is NaN or empty
    sizes_list = sizes_list.dropna().apply(lambda x: [item for item in x if item])
    
    # Repeat the rows based on the number of sizes in each row
    df = df.loc[sizes_list.index.repeat(sizes_list.str.len())].reset_index(drop=True)
    
    # Flatten the list of sizes and extract the part before the slash
    df['size'] = [size.split('/')[0] for sublist in sizes_list for size in sublist]
    
    # Drop duplicates to get unique variantid and size combinations
    return df[['variantid', 'size']].drop_duplicates()

def import_data():
    class Config:

        CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K";
        CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c";
        REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce";

        ORGID = "60006357703";
        WORKSPACEID = "174857000004732522";
        VIEWID = "174857000102069059";

    class sample:

        ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN)

        def import_data(self, ac):
            import_type = "truncateadd"
            file_type = "csv"
            auto_identify = "true"
            file_path = 'VariantSizeList.csv'
            bulk = ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)
            result = bulk.import_data(Config.VIEWID, import_type,file_type, auto_identify, file_path)        
            print(result)

    try:
        obj = sample()
        obj.import_data(obj.ac);

    except Exception as e:
        print(str(e))



if __name__ == "__main__": 
    job_id = create_jobid_loop()  
    get_jobid_loop(job_id)  
    export(job_id)
    df = pd.read_csv('VariantSizeText.csv')
    
    # Extract unique sizes for each variantid
    unique_sizes_df = extract_unique_sizes(df)
    # unique_sizes_df = pd.read_csv('VariantSizeList.csv')
    combined_sizes_df = unique_sizes_df.groupby('variantid')['size'].apply(lambda x: ','.join(sorted(x.unique()))).reset_index()
    
    # Save the result to a CSV file
    # unique_sizes_df.to_csv('VariantSizeList.csv', index=False)
    combined_sizes_df.to_csv('VariantSizeList.csv', index=False)
    
    print(combined_sizes_df)
    
    print(unique_sizes_df)
    import_data()
