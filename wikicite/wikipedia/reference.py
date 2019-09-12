import json
import dateutil.parser
from typing import Dict


class ReferenceError(Exception):
    pass


class References(object):
    def __init__(self, references: Dict[int, 'Reference']) -> None:
        self.references = references

    def to_json(self):
        return {key: value.to_json() for key, value in self.references.items()}


class Reference(object):
    def __init__(self, id_, template):
        self.id = id_
        self._parse_type(template)
        self._parse_params(template)
        self._parse_title()
        self._parse_url()
        self._parse_date()

    def _parse_type(self, template):
        try:
            self.type = template['parts'][0]['template']['target']['wt'].strip().lower()
        except (KeyError, IndexError):
            raise ReferenceError(template)

    def _parse_params(self, template):
        self.params = {}
        try:
            for param, value in template['parts'][0]['template']['params'].items():
                try:
                    self.params[param] = value['wt'].strip()
                except KeyError:
                    pass
        except (KeyError, IndexError):
            pass

    def _parse_title(self):
        self.title = None
        for key in ['title']:
            if key in self.params:
                self.title = self.params[key].strip()
                break

    def _parse_url(self):
        self.url = None
        for key in ['archiveurl', 'archive-url', 'url']:
            if key in self.params:
                self.url = self.params[key].strip()
                break

    def _parse_date(self):
        self.date = None
        for key in ['archivedate', 'archive-date', 'accessdate', 'access-date', 'date']:
            if key in self.params:
                if self.params[key]:
                    try:
                        self.date = dateutil.parser.parse(self.params[key])
                        break
                    except (ValueError, OverflowError):
                        pass

    def to_json(self):
        data = {}
        if self.type:
            data['type'] = self.type
        if self.title:
            data['title'] = self.title
        if self.url:
            data['url'] = self.url
        if self.date:
            data['date'] = str(self.date)
        return data


def parse_references(tree):
    references = {}
    for i, reference_node in enumerate(tree.xpath("//ol[contains(@class, 'mw-references')]/li")):
        children = reference_node.xpath('span[@class="mw-reference-text"]/span[@data-mw]')
        if children:
            # Sometimes there may be more than one valid child (see Obama #136)
            # For now, we take the first valid child as the reference.
            node = children[0]
            template = json.loads(node.get('data-mw'))
            try:
                reference = Reference(i + 1, template)
                references[i + 1] = reference
            except ReferenceError:
                pass
        else:
            # This a raw text citation with no template
            pass

    return References(references)
