import re
from collections import namedtuple

Citation = namedtuple('Citation', ['reference_id', 'offset'])

_heading_tags = set(['h2', 'h3', 'h4', 'h5', 'h6'])


class Headings(object):
    def __init__(self):
        self.headings = [''] * len(_heading_tags)
        self.previous_level = 0

    def add(self, header, level):
        self.headings[level - 1] = header.strip()
        for h in range(level, len(self.headings)):
            self.headings[h] = ''

        if level >= self.previous_level:
            self.previous_level = level
            return True
        return False

    def copy(self):
        return self.headings[:]

    def __str__(self):
        return str(self.headings)

    def to_json(self):
        return self.headings


class Article(object):
    def __init__(self, title: str, page_id: int):
        self.title = title
        self.page_id = page_id
        self.sections = []

    def is_empty(self):
        return len(self.sections) == 0

    def to_json(self):
        return {
            'title': self.title,
            'page_id': self.page_id,
            'sections': [section.to_json() for section in self.sections]
        }


class Section(object):
    def __init__(self, id_, headings):
        self.id = id_
        self.headings = headings
        self.paragraphs = []

    def __bool__(self):
        return len(self.paragraphs) > 0

    def to_json(self):
        return {
            'id': self.id,
            'headings': self.headings,
            'paragraphs': [paragraph.to_json() for paragraph in self.paragraphs]
        }


class Paragraph(object):
    def __init__(self, id_, text, sentence_offsets, citations):
        self.id = id_
        self.text = text
        self.sentence_offsets = sentence_offsets
        self.citations = citations

    def to_json(self):
        return {
            'id': self.id,
            'text': self.text,
            'sentence_offsets': self.sentence_offsets,
            'citations': [
                {
                    'reference_id': citation.reference_id,
                    'offset': citation.offset
                }
                for citation in self.citations
            ]
        }


def _get_header_level(node):
    if node.tag == 'h2': return 1
    if node.tag == 'h3': return 2
    if node.tag == 'h4': return 3
    if node.tag == 'h5': return 4
    if node.tag == 'h6': return 5
    return None


def _parse_text(node):
    text = []
    citations = []
    offset = 0
    for t in node.itertext():
        if t.startswith('{{'):
            continue
        match = re.match('\[(\d+)\]', t)
        if match:
            reference_id = int(match.group(1))
            citations.append(Citation(reference_id, offset))
        else:
            text.append(t)
            offset += len(t)

    text = ''.join(text)
    return text, citations


def parse_article(nlp, title: str, page_id: int, tree):
    body = tree.xpath('body')[0]

    article = Article(title, page_id)
    headings = Headings()
    section = Section(len(article.sections), headings.copy())
    has_list = False
    for node in body.getchildren():
        if node.tag in ['ul']:
            has_list = True
        if node.tag in ['p']:
            text, citations = _parse_text(node)
            text = text.strip()
            if text:
                # Keep offsets as list to be consistent with the json deserialization
                sentence_offsets = [[sent.start_char, sent.end_char] for sent in nlp(text).sents]
                paragraph_id = len(section.paragraphs)
                paragraph = Paragraph(paragraph_id, text, sentence_offsets, citations)
                section.paragraphs.append(paragraph)
        elif node.tag in _heading_tags:
            level = _get_header_level(node)
            if section and not has_list:
                article.sections.append(section)
            heading = node.text_content()
            headings.add(heading, level)
            section = Section(len(article.sections), headings.copy())
            has_list = False

    if section:
        article.sections.append(section)
    return article
