import requests,json,time,math,re,csv
class Result:
    def __init__(self,nextUri,timeout = 5):
        self.__nextUri = nextUri
        self.timeout = timeout
        self.infoUri = None
        self.__response = None
        self.__status = None
        self.__used_time = None
    
    def __prettify_response(self,response):
        if self.__status == 'FAILED':
            return "{} {}".format("FAILED",response['error']['message'])
        elif self.__status == 'FINISHED':
            column_name = ""
            for col in response['columns']:
                column_name += col['name']
                column_name += '\t'
            if 'data' in response:
                return "{} \n{}\n{}".format(self.__status,column_name,response['data'])
            return "{} \n{}".format(self.__status,column_name)
    
    def __get_result_immediately(self):
        response = requests.get(self.__nextUri)
        response_dict = json.loads(response.text)
        if 'nextUri' not in response_dict:
            self.__response = response_dict
            self.__status = response_dict['stats']['state']
            self.infoUri = response_dict['infoUri']
            return response_dict
        else:
            self.__nextUri = response_dict['nextUri']
            return None
        
    def get_result(self,timeout = None):
        if self.__response is not None:
            return self.__response
        if timeout is None:
            timeout = self.timeout
        use_time = 0
        interval = 0.5
        while True:
            result = self.__get_result_immediately()
            if result is not None:
                return result
            if use_time > timeout:
                break
            use_time += interval
            time.sleep(interval)
            interval = math.ceil(use_time/10)
        return None
        
    def print_result(self):
        response = self.get_result()
        print(self.__prettify_response(response))

    def get_used_time(self,timeout = None):
        if self.__used_time is not None:
            return self.__used_time
        response = self.get_result(timeout)
        if self.__status == "FINISHED":
            elapsedTimeMillis = response['stats']['elapsedTimeMillis']
        else:
            print(response['error']['message'])
            return 0
        return elapsedTimeMillis
    def get_infoUri(self,timeout = None):
        self.get_result(time)
        return self.infoUri

class WebResult:
    def __init__(self,uuid,Client):
        self.uuid = uuid
        self.client = Client
        self.result = None
        self.finished = False # 没运行完和执行失败时都是False
        self.used_time = None
        self.infoUri = None
        self.csv_path = None
    def __get_result_immediately(self):
        result = self.client.get_query(self.uuid)
        if result is not None:
            #print(result)
            if result['state'] == 'FINISHED':
                self.finished = True
                self.used_time = result['queryStats']['elapsedTime']
                self.infoUri = "http://{}:{}/ui/{}".format(self.client.host,self.client.port,result['infoUri'])
            elif result['state'] == 'FAILED':
                print(result['error']['message'])
            else:
                return None
        return result
            
    def get_result(self,timeout = None):
        if self.result is not None:
            return self.result
        if timeout is None:
            timeout = 5
        use_time = 0
        interval = 0.5
        while True:
            result = self.__get_result_immediately()
            if result is not None:
                self.result = result
                return result
            if use_time > timeout:
                break
            use_time += interval
            time.sleep(interval)
            interval = math.ceil(use_time/10)
        return None
    def get_used_time(self,timeout = None):
        self.get_result(timeout)
        #print(self.used_time)
        if "ms" in self.used_time:
            return float(re.sub("ms","",self.used_time))/1000
        elif "s" in self.used_time:
            return  float(re.sub("s","",self.used_time))
        else:
            return  float(re.sub("m","",self.used_time))*60
    def get_infoUri(self,timeout = None):
        self.get_result(timeout)
        return self.infoUri
    def get_output(self,timeout = None):
        csv_path = self.get_csv_path(timeout = None)
        if csv_path is not None:
            print_csv(csv_path)
    def get_csv_path(self,timeout = None):
        if self.csv_path is not None:
            return self.csv_path
        self.get_result(timeout)
        if self.finished is False:
            return None
        elif self.result['output']['type'] == 'csv':
            self.csv_path = "http://{}:{}{}".format(self.client.host,self.client.port,self.result['output']['location'][2:])
            return self.csv_path
        else:
            print("output is {} Type".format(self.result['output']['type']))
            return None

def print_csv(csv_path):
    res = requests.get(csv_path)
    content = res.content.decode().strip()
    print(content.replace('"','').replace(',',' | '))

class Client:
    def __init__(self,host="127.0.0.1",port=8080,user="lk",catalog="system",schema="runtime",timeout = 10000):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.catalog = catalog
        self.user = user
        self.schema = schema
        self.execute_url = "http://{}:{}{}".format(self.host,self.port,"/v1/statement")
        self.web_execute_url = "http://{}:{}{}".format(self.host,self.port,"/api/execute")
        self.get_history_url = "http://{}:{}{}".format(self.host,self.port,"/api/query/history")
        self.headers = {
            "X-Presto-Catalog":catalog,
            "X-Presto-Schema":schema,
            "X-Presto-User":user,
            "X-Presto-Source":"python_driver",
            "source":"python_web_driver",
            "Content-Type":"application/json"
        }
    def execute(self,sql):
        sql = sql.split(';')[0]
        response = requests.post(self.execute_url,data = sql,headers = self.headers)
        if response.ok:
            return Result(json.loads(response.text)['nextUri'])
        else:
            return response.text
    def web_execute(self,sql):
        sql = sql.split(';')[0]
        payload = {
            "query":sql,
            "sessionContext": {
                "catalog":self.catalog,
                "schema":self.schema
            }
        }
        payload_str = json.dumps(payload)
        #print(payload_str)
        response = requests.put(url = self.web_execute_url,data = payload_str,headers = self.headers)
        self.uuid = json.loads(response.text)[0]['uuid']
        return WebResult(self.uuid,self)
    def get_all_query(self):
        response = requests.get(self.get_history_url,headers = self.headers)
        query_list = json.loads(response.text)
        return query_list
    def get_query(self,uuid):
        query_list = self.get_all_query()
        for query in query_list:
            if query['uuid'] == uuid:
                return query
        return None

if __name__ == "__main__":
    client = Client(host='192.168.40.152',port=18080,user='lk',catalog='clickhouse152',schema='ssb')
    result = client.web_execute("SELECT sum(LO_REVENUE),  year(LO_ORDERDATE) AS year,  P_BRAND  FROM lineorder_flat  WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'  GROUP BY  year(LO_ORDERDATE),  P_BRAND  ORDER BY  year(LO_ORDERDATE),  P_BRAND LIMIT 3")
    result.get_output()