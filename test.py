import hashlib


md5_hash = hashlib.md5()
md5_hash.update( open("client_0"+".txt", 'rb').read())
hash = md5_hash.hexdigest()
print("md5 sum:",hash)