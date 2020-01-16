import google_translator as translator

TWITTER_PARAMS = {
    "IT_QUERY_FILTER": "-has:media place_country:IT",
    "IT_EN_QUERY_FILTER": "-has:media place_country:IT lang:en",
    "GENERIC_QUERY_FILTER": "-has:media",
    "RESULTS_PER_CALL": 1,  # 100,
    "MAX_RESULTS": 1  # 100
}

NEWS_PARAMS = {
    "page_size": 1  # 100
}

QUERIES = ["early election", "snap election", "government collapse",
           "government coalition", "election", "instability", "uncertainty",
           "crisis", "coalition"]

QUERIES_IT = [q for q in map(lambda s: translator.translate_keyword(s, "it"),
                             QUERIES)]

FROM_USERS = ["lorepregliasco", "FerdiGiugliano", "AlbertoNardelli",
              "gavinjones10"]

NEWS_SOURCES = ",".join(["reuters", "ansa", "google-news-it"])

BERT_MODEL = {
    "endpoint_uri": "http://ac6a2064dee3c11e99ced0a13821e56d-733867741.ap"
                    "-southeast-1.elb.amazonaws.com/sentiment/classifier",
    "headers": {"content-type": "application/json"}
}
USERNAME = "root"
PASSWORD = "Gradient8#"
HOST = "127.0.0.1"
PORT = "3306"
SCHEMA = "search_automation"
