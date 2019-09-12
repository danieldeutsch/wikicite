import json
import sys
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException


def main():
    for line in sys.stdin:
        data = json.loads(line)
        english_documents = []
        for document in data['documents']:
            text = ' '.join([sentence for paragraph in document['paragraphs'] for sentence in paragraph])
            try:
                if detect(text) == 'en':
                    english_documents.append(document)
            except LangDetectException:
                pass
        if len(english_documents) > 0:
            data['documents'] = english_documents
            print(json.dumps(data))


if __name__ == '__main__':
    main()
