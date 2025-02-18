import hashlib


def calculate_md5(input_string):
    """
    文字列のMD5ハッシュを計算する関数

    Args:
        input_string (str): ハッシュ値を計算したい文字列

    Returns:
        str: 16進数形式のMD5ハッシュ値
    """
    # 文字列をバイト列にエンコード
    encoded_string = input_string.encode('utf-8')

    # MD5ハッシュオブジェクトを作成
    md5_hash = hashlib.md5()

    # データを更新
    md5_hash.update(encoded_string)

    # 16進数形式のハッシュ値を取得
    return md5_hash.hexdigest()


# 使用例
if __name__ == "__main__":
    test_string = "Hello, World!"
    hash_value = calculate_md5(test_string)
    print(f"String: {test_string}")
    print(f"MD5 Hash: {hash_value}")
