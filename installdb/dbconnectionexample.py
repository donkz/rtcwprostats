#python setup
#pip install mysqlclient

#mysql setup
#CREATE USER 'test3'@'myip' IDENTIFIED BY 'passwordxyz';
#GRANT CREATE, INSERT, DELETE, UPDATE, SELECT ON rtcwprostats.* TO 'test3'@'myip';
#SHOW GRANTS FOR 'test3'@'myip';
#FLUSH PRIVILEGES;

def get_db_connection_string(environment = "test"):
    test_driver = "sqlite"
    test_path = "..//test//database.db"
    
    prod_driver = "mysql"
    prod_user = "test"
    prod_password = "passwordxyz"
    prod_endpoint = "db.rds.amazonaws.com"
    prod_port = "3306"
    prod_db = "rtcwprostats"
    
    if environment == "test":
        #example: 'sqlite:///..//test//database.db'
        return f'{test_driver}:///{test_path}'
        
    elif environment == "prod":
        #example: 'mysql://test:password11@db.rds.amazonaws.com:3306/rtcwprostats'
        return f'{prod_driver}://{prod_user}:{prod_password}@{prod_endpoint}:{prod_port}/{prod_db}'
    else:
        raise Exception("Unknown db environment")
        
        
        
