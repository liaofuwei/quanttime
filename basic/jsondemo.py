import json


with open("..\\config.json", "r") as f:
    aa = json.loads(f.read())
print aa["security_data_dir"]

'''

a = {"name":"C:\quanttime", "security_data_dir":"E:\quanttime\data\security_raw"}
with open("E:\\quanttime\\config1.json", "w") as f:
    f.write(json.dumps(a, indent=4))
'''