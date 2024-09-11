
import json


# 按字典值value递减排序
def sortCustomDict(data: dict, reverse=True) -> dict:
    return dict(sorted(data.items(), key=lambda x: x[1], reverse=reverse))

def Dict2Json(outfile, data):
    with open(outfile,'w') as f:
        json.dump(data, f)

def Json2Dict(file):
    with open(file, 'r') as f:
        ans = json.load(f)
    return ans
