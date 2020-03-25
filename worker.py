from typing import List, Tuple
import string
from decimal import Decimal

from gensim import models
from gensim.corpora import Dictionary
from gensim.interfaces import TransformedCorpus
from gensim.models import LsiModel

import nltk
from nltk import tokenize
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords

nltk_data_dir: str = '/tmp/nltk_data'
nltk.download('punkt', download_dir=nltk_data_dir)
nltk.download('stopwords', download_dir=nltk_data_dir)
nltk.data.path.append(nltk_data_dir)


def vectorize(sentences: List[str]) -> List[List[Tuple[int, Decimal]]]:

    words_list: List[List[str]]
    # 単語に分割
    words_list = list(tokenize.word_tokenize(sentence) for sentence in sentences)

    # 単語の語頭・語尾の記号を除去
    words_list = list(list(word.strip(f'{"".join(string.punctuation)}') for word in words) for words in words_list)

    # ステミング（goes, pencils => go, pencilのように、語幹部分に変換する）
    ps: PorterStemmer = PorterStemmer()
    words_list = list(list(ps.stem(word) for word in words) for words in words_list)

    # ストップワード除去（the, a, ofなどの特徴にならない単語を除く）
    stop_words: List[str] = stopwords.words('english')
    exclude_words: List[str] = stop_words + list(string.ascii_lowercase) + list(string.digits) + list(string.punctuation) + ['``']
    words_list = list(list(word for word in words if word not in exclude_words) for words in words_list)

    # 辞書を作成する（単語 -> ID）
    dictionary: Dictionary = Dictionary(words_list)

    # コーパス化（各文書に含まれる単語群を、Tuple[ID, 出現回数]のリストに変換）
    corpus: List[List[Tuple[int, int]]] = list(map(dictionary.doc2bow, words_list))

    # TF-IDF modelの生成
    tfidf_model: models.TfidfModel = models.TfidfModel(corpus)

    # corpusへのモデル適用
    tfidf_corpus: TransformedCorpus = tfidf_model[corpus]

    # LSI modelの生成
    lsi_model: LsiModel = LsiModel(corpus=tfidf_corpus, id2word=dictionary, num_topics=300)

    # LSIモデルを適用
    lsi_corpus: TransformedCorpus = lsi_model[tfidf_corpus]

    # リスト形式に変更
    sentence_vectors: List[List[Tuple[int, Decimal]]] = list(
        c for c in lsi_corpus
    )

    return sentence_vectors
