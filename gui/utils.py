import pandas as pd
import hashlib


def create_df_ips(connection):
    # Form the string to display the IP addresses on the interface.
    df_ips = pd.read_sql(sql="SELECT * FROM ips", con=connection)
    df_ips['type'] = df_ips['type'].astype(int).astype(str)
    df_ips['broker_id'] = df_ips['broker_id'].astype(int).astype(str)
    
    print(f"df_ips:\n\n{df_ips}\n\n")
    
    df_ips[df_ips['type']=='0']['broker_ip'].iloc[0]

    display_ips = 'Kafka Cluster IPs:\n\n'
    display_ips += f"zookeeper ID: {df_ips[df_ips['type']=='0']['broker_ip'].iloc[0]}\n"

    df_ips_br = df_ips[df_ips['type']=='1']
    df_ips_br = df_ips_br.sort_values(by=['broker_id'], ascending=[True])

    for i in range(len(df_ips_br)):
        display_ips += f"broker_{df_ips_br['broker_id'].iloc[i]} ID: {df_ips_br['broker_ip'].iloc[i]}\n"
    
    return display_ips, df_ips
    


class PasswordHash:
    
    def password_hash(self, password):
        # Hash the user password
        password_obj = hashlib.sha512()
        
        password_obj.update(password.encode('utf-8'))
        password = password_obj.hexdigest()
        
        # Hash the hash of the previous password.
        password_obj.update(password.encode('utf-8'))
        password = password_obj.hexdigest()
        
        return password
    
    def verify(self, db_pwd_hash, password):
        
        password = self.password_hash(password)
        
        if db_pwd_hash == password:
            return True
        else:
            return False
