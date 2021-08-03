#
# A Python module for converting a CDE question text into tags
#

import re


class Tags:
    @staticmethod
    def expand_text(text):
        if text == 'rx':
            return 'prescription'

        if text == 'hx':
            return 'history'

        # Could not expand.
        return text

    @staticmethod
    def question_text_to_tags(question_text: str):
        question_text_lower = question_text.lower()

        # Identify phrases of interest.
        tag_phrases = list()
        re_phrases = [
            r'(\w+\s+to\s+\w+)',        # ... to ...
            r'"(.*)"',                  # "..."
            r"'(.*)'",                  # '...'
            r"(on\s+\w+)",              # on ...
            r"(at\s+\w+)",              # at ...
        ]
        for re_phrase in re_phrases:
            match = re.search(re_phrase, question_text_lower)
            if match:
                tag_phrases.append(match.group(1))

        # Generate a list of all tags.
        no_quotes = question_text_lower.replace("'", '').replace('"', '')
        all_tags = sorted(re.split('\\W+', no_quotes))

        # Uniqify.
        uniq_tags = set()
        uniq_tags.update(all_tags)
        uniq_tags.update(tag_phrases)

        # Remove blanks.
        uniq_tags_no_blanks = filter(
            lambda wd: wd.strip() != '',
            list(uniq_tags)
        )

        return sorted(uniq_tags_no_blanks)

    @staticmethod
    def sort_search_results(all_tags, rows):
        # TODO: prioritize rarer words
        return sorted(rows, key=lambda row: len(list(set(row['tags']) & set(all_tags))), reverse=True)
