"""
Willy AU
Basic Hadoop Exercice
Python 3.4.5
"""

import numpy as np
punct = [',', ':', '!', '?', '.', ';', '\n', "'", '"', ' ']


def wordcount_map(kval_txt):
    """
    Map job that processes lines of text
    :param kval_txt: list of key-value pair (line number, text)
    :return kval_word: list of key-value pair (word, 1)
    """
    # empty list
    kval_word = []

    # for each line (key), process the text (value)
    for kval in kval_txt:
        for word in kval[1].split(sep=' '):

            # basic processing of punctuations and upper cases
            for e in punct:
                word = word.replace(e, '').lower()

            # append key-value pair (word, 1) to list
            kval_word.append((word, 1))

    return kval_word


def wordcount_reduce(kval_word):
    """
    Reduce job that counts words
    :param kval_word: list of key-value pair (word, list of integer)
    :return: kval_count: list of key-value pair (word, count)
    """
    # empty list
    kval_count = []

    # for each word (key), aggregate counts (value) by summing
    for kval in kval_word:

        # append new key-value pair to list
        kval_count.append((kval[0], np.sum(kval[1])))

    return kval_count


def wordcount_shuffle(kval_word):
    """
    Shuffle that gather counts in list of counts
    :param kval_word: list of key-value pair (word, count)
    :return: list of key-value pair (word, list of count)
    """
    # empty dictionary (hash map)
    kval_dict = dict()

    # fill dictionary
    for kval in kval_word:
        key, count = kval[0], kval[1]
        if key not in kval_dict:
            # create list if key not in dictionary
            kval_dict[key] = [count]
        else:
            # insert new count in list
            kval_dict[key].append(count)

    # construct list of key-value pair from dict
    return [(k, v) for k, v in zip(kval_dict.keys(), kval_dict.values())]


if __name__ == '__main__':

    # path of text
    txt_path = 'lorem.txt'

    # create list of key-value pair (line number, text)
    kval_txt = [(i, text.rstrip('\n').rstrip(' ')) for i, text in enumerate(open(txt_path))]

    # use map function
    kval_word = wordcount_map(kval_txt)

    # shuffle
    kval_word_sorted = wordcount_shuffle(kval_word)

    # sort using default order
    kval_word_sorted.sort()

    # use reduce job
    kval_count = wordcount_reduce(kval_word_sorted)

    for pair in kval_count:
        print(pair)