#
# A Python module for converting a CDE question text into tags
#

import re

import couchdb


class Tags:
    tags_to_be_deleted = [
        ''
    ]

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

        # Remove tags to be deleted.
        for tag in Tags.tags_to_be_deleted:
            if tag in uniq_tags:
                uniq_tags.remove(tag)

        # Remove blanks.
        uniq_tags_no_blanks = filter(
            lambda wd: wd.strip() != '',
            list(uniq_tags)
        )

        return sorted(uniq_tags_no_blanks)

    @staticmethod
    def sort_search_results(all_tags, query_tags, rows):
        def sort_map(row):
            overlapping_tags = list(set(row['tags']) & set(query_tags))

            # The score depends on how rare the overlapping words is in all_tags.
            score = 0
            for tag in overlapping_tags:
                if tag in all_tags:
                    score += all_tags[tag]

            print(f"Overlapping tags: {overlapping_tags} (score: {score})")

            return score

        # TODO: prioritize rarer words
        return sorted(rows, key=sort_map)

    @staticmethod
    def generate_tag_counts(db: couchdb.Database, partition: str):
        # TODO: figure out how to search just for the partition
        results = db.find({
            "selector": {
                "tags": {
                    "$exists": True
                }
            },
            "fields": [
                "tags"
            ],
            "limit": 1000000
        })

        dict_counts = {}
        for result in results:
            print(result)
            tags = result['tags']
            for tag in tags:
                if tag not in dict_counts:
                    dict_counts[tag] = 0
                dict_counts[tag] += 1

        # Clean some worthless dict counts.
        for tag in Tags.tags_to_be_deleted:
            if tag in dict_counts:
                del dict_counts[tag]

        # Calculate the total number of tag occurrences.
        dict_counts['*'] = sum(dict_counts.values())

        return dict_counts
