#
# A Python module for converting a CDE question text into tags
#
import math
import re
import logging

import couchdb


class Tags:
    tags_to_be_deleted = [
        '',
        'the',
        'and',
        'to',
        'up',
        'on',
        'a',
        'like',
        'or',
        'i',
        'you',
        'my',
        'your'
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
        min_tag_freq = all_tags[min(all_tags, key=all_tags.get)]
        max_tag_freq = all_tags[max(all_tags, key=all_tags.get)]

        def tag_score(tag):
            if tag not in all_tags:
                return 0

            freq_tag = all_tags[tag]

            # Rescale to min:max
            rescaled_freq = (freq_tag - min_tag_freq)/(max_tag_freq - min_tag_freq)

            # Do an inverse transform so that the most rare are disproportionately higher scored.
            score = 1 - math.tan(rescaled_freq)

            logging.debug(f"Tag '{tag}' with frequency {freq_tag} ({rescaled_freq} between {min_tag_freq} to {max_tag_freq}) scored as {score}")

            return score

        def score_rows(row):
            set_row_tags = set(row['tags'])
            set_query_tags = set(query_tags)
            overlapping_tags = list(set_row_tags & set_query_tags)

            # If we only have a single overlapping tag, it can't *possibly* be relevant.
            # So let's just skip it.
            if len(overlapping_tags) < 2:
                return 0

            # Here's how we calculate the score:
            #   1. Every tag counts for UP TO 1 point each, additively.
            #   2. A rare tag gets 1.0 points, a common tag gets 0.0 points.

            # The score depends on how rare the overlapping words is in all_tags.
            score = 0
            for tag in overlapping_tags:
                if tag in all_tags:
                    score += tag_score(tag)

            logging.debug(f"Overlapping tags between row {set_row_tags} and query {set_query_tags}: {overlapping_tags}")
            logging.info(f"Scored overlapping tags {overlapping_tags} (score: {score})")

            return score

        # TODO: prioritize rarer words
        scored_rows = [dict(row, score=score_rows(row)) for row in rows]
        return sorted(scored_rows, key=lambda row: row['score'], reverse=True)

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
            # print(result)
            tags = result['tags']
            for tag in tags:
                if tag not in dict_counts:
                    dict_counts[tag] = 0
                dict_counts[tag] += 1

        # Clean some worthless dict counts.
        for tag in Tags.tags_to_be_deleted:
            if tag in dict_counts:
                del dict_counts[tag]

        return dict_counts
