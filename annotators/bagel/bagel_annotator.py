import functools
import logging

from renci_ner.core import AnnotatorWithProps, Annotation
from renci_ner.services.linkers.babelsapbert import BabelSAPBERTAnnotator
from renci_ner.services.linkers.bagel import BagelAnnotator
from renci_ner.services.linkers.nameres import NameRes
from renci_ner.services.ner.biomegatron import BioMegatron


# Annotate a CRF in our weird internal format using Bagel.


# Ignore certain concepts and categories.
IGNORED_CONCEPTS = {
    'UBERON:0002542',                   # "scale" -- not a body part!
    'UBERON:0007380',                   # "dermal scale" -- not a body part!
    'UMLS:C3166305',                    # "Scale used for body image assessment"
    'MONDO:0019395',                    # "have" is not Hinman syndrome
    'UBERON:0006611',                   # "test" is not an exoskeleton
    'PUBCHEM.COMPOUND:135398658',       # "being" is not folic acid
    'MONDO:0010472',                    # "being" is not developmental and epileptic encephalopathy
    'MONDO:0011760',                    # "being" is not Scheie syndrome
    'MONDO:0008534',                    # "getting" is not generalized essential telangiectasia
    'UBERON:0004704',                   # "depression" is not bone fossa
    'UBERON:0000467',                   # "system" is not an anatomical system
    'MONDO:0017879',                    # "have" is not hantavirus pulmonary syndrome
    'MONDO:0009271',                    # "going" or "goes" is not geroderma osteodysplastica
    'MONDO:0017015',                    # "children" is not primary interstitial lung disease specific to childhood
    'CHEBI:24433',                      # "rested" or "group" aren't a chemical group
    'MONDO:0010953',                    # "face" is not Fanconi anemia complementation group E
    'GO:0043336',                       # "rested" is not site-specific telomere resolvase activity
    'NCBIGene:5978',                    # "rested" is not the gene REST
    'MONDO:0009176',                    # "ever" is not epidermodysplasia verruciformis
    'GO:0019013',                       # "core" is not viral nucleocapsid
    'MONDO:0012833',                    # "could" is not Crouzon syndrome-acanthosis nigricans syndrome (): 18 CRFs
    'NCBITaxon:6754',                   # "cancer" is not the animal group Cancer
    'UBERON:0004529',                   # "spine" is not anatomical projection
    'UBERON:0013496',                   # "spine" is not unbarbed keratin-coated spine
    'MONDO:0000605',                    # "sensitive" is not hypersensitivity reaction disease
    'PUBCHEM.COMPOUND:84815',           # "meds" is not D-Methionine
    'MONDO:0016648',                    # "meds" is not multiple epiphyseal dysplasia (disease)
    'GO:0044326',                       # "neck" is not dendritic spine neck
    'UBERON:2002175',                   # "role" is not rostral octaval nerve motor nucleus
    'MONDO:0002531',                    # "skin" is not skin neoplasm
    'MONDO:0024457',                    # "plans" is not neurodegeneration with brain iron accumulation 2A
    'MONDO:0017998',                    # "plans" is not PLA2G6-associated neurodegeneration
    'UBERON:0004111',                   # "open" is not anatomical conduit
    'MONDO:0002169',                    # "read" is not rectum adenocarcinoma
    'UBERON:0007358',                   # "read" is not abomasum
    'CHEBI:18282',                      # "based" is not nucleobase
    'UBERON:0035971',                   # "post" is not postsubiculum
    'GO:0033867',                       # "fast" is not Fas-activated serine/threonine kinase activity
    'CHEMBL.COMPOUND:CHEMBL224120',     # "same" is not CHEMBL224120
    'MONDO:0019380',                    # "weed" is not western equine encephalitis
    'CL:0000968',                       # "cell" is not Be cell
    'PR:000004978',                     # "calm" is not calmodulin
    'GO:0008705',                       # "meth" is not methionine synthase activity
    'UBERON:0001932',                   # "arch" is not arcuate nucleus of hypothalamus
    'MONDO:0004980',                    # "allergy" is not atopic eczema
    'MONDO:0009994',                    # "arms" are not alveolar rhabdomyosarcoma
    # Stopped at decompression sickness (MONDO:0020797): 5 CRFs
    'PUBCHEM.COMPOUND:5460341',         # "could" is not Calcium: 19 CRFs
    'GO:0044309',                       # "neuron spine" isn't right -- "spine" should match UBERON:0001130 (and does)
    'UBERON:0015230',                   # "dorsal vessel heart" isn't as good as "heart" (UBERON:0000948), which we match
    'KEGG.COMPOUND:C00701',             # "based" is not a base
    'UBERON:0010230',                   # "eyeball of camera-type eye" is probably too specific
    'PUBCHEM.COMPOUND:34756',           # "same" is not S-Adenosyl-L-methionine (PUBCHEM.COMPOUND:34756): 8 CRFs
    'CL:0000000',                       # "cell" never refers to actual cells
    'CL:0000669',                       # "pericyte cell" never refers to actual cells
    'PUBCHEM.COMPOUND:5234',            # Both mentions of 'salts' refer to the drug "bath salts" (https://en.wikipedia.org/wiki/Bath_salts)
    'GO:0031672',                       # The "A band" cellular component doesn't really come up here


    # TODO:
    # - chronic obstructive pulmonary disease (MONDO:0005002): 17 CRFs -> matches "cold"
    # - leg (UBERON:0000978) -> matches "lower extremity"
    # - Needs more investigation:
    #   - hindlimb zeugopod (UBERON:0003823): 14 CRFs
    #   - heme (PUBCHEM.COMPOUND:53629486 <- Molecular Mixture)
    # Stopped at forelimb stylopod (UBERON:0003822): 10 CRFs
}

def get_designation(element):
    """ Return the designations for a CDE. If any designations are present, we concatenate them together so they can be
    passed to the Monarch API in a single API call.
    """
    if 'designations' in element:
        return '; '.join(map(lambda d: d['designation'], element['designations']))
    else:
        return ''

bagel_annotator = None

@functools.lru_cache(maxsize=4096)
def ner_via_bagel(crf_id, text, sapbert_score_threshold=0.8):
    """
    Query the Bagel NER system to look up concepts for a particular text.

    :param text: The text to run NER on
    :return: The response from the NER service, translated into token definitions.
    """

    global biomegatron
    global sapbert_annotator
    global nameres_annotator
    global bagel_annotator
    if not bagel_annotator:
        biomegatron = BioMegatron()
        nameres_annotator = NameRes()
        sapbert_annotator = BabelSAPBERTAnnotator()
        bagel_annotator = BagelAnnotator()

    errors = []
    result = None
    try:
        annotated_text = biomegatron.annotate(text)
        result = bagel_annotator.annotate_with(annotated_text, [
            AnnotatorWithProps(nameres_annotator, {
                'limit': 5,
                'only_taxa': [
                    'NCBITaxon:9606',       # Homo sapiens
                    'NCBITaxon:10090',      # Mus musculus
                    'NCBITaxon:10116',      # Rattus norvegicus
                    'NCBITaxon:7955'        # Danio rerio
                ]
            }),
            AnnotatorWithProps(sapbert_annotator, {
                'limit': 5,
                'score': sapbert_score_threshold,
            })
        ])
    except ValueError as err:
        logging.error(f"Could not annotate \"{text[0:100]}...\" using BioMegatron + Bagel: {err}")
        errors.append(str(err))

    if not result:
        denotations = []
    else:
        denotations = list(map(lambda d: {
            'text': d.text,
            'start': d.start,
            'end': d.end,
            'concept': {
                'id': d.id,
                'label': d.label,
                'biolink_type': d.type,
                'score': d.props.get('score', None),
            }
        }, result.annotations))

    return {
        'denotations': denotations,
        'errors': errors,
        'ignored': [],
        'normalized': []
    }


# Number of associations in this file.
association_count = 0
# Numbers of errors (generally terms without a valid ID).
count_errors = 0
# Terms ignored.
ignored_count = 0
# Elements processed
count_elements = 0
# Tokens identified
count_tokens = 0
# Tokens normalized
count_normalized = 0
# Tokens that could not be normalized
count_could_not_normalize = 0
# Normalized tokens that were ignored as per the ignore list.
count_ignored = 0

def annotate_crf(graph, crf_id, crf, source, add_cde_count_to_description=False, sapbert_score_threshold=0.8):
    """
    Annotate a CRF. We need to recursively annotate the CDEs as well.

    :param graph: A KGX graph to add the CRF to.
    :param crf: The CRF in JSON format to process.
    :param source: The source of this data as a string.
    :return: A 'comprehensive' JSON object representing this file -- this is the input JSON file with
    the annotations added on. It also modifies graph and writes outputs to STDOUT (Disgusting!).
    """
    global count_elements
    global count_tokens
    global count_normalized
    global count_could_not_normalize
    global count_ignored

    designation = get_designation(crf)

    # We expect a title and a description.
    # crf_name = crf['titles'][0]
    # if not crf_name:
    #     crf_name = "(untitled)"
    crf_name = crf_id
    if crf_name.startswith("HEALCDE:"):
        crf_name = crf_name[8:]
    description = crf['descriptions'][0]
    if not description:
        description = ""

    # Generate text for the entire form in one go.
    crf_text = designation + "\n" + crf_name + "\n" + description + "\n"
    count_cdes = 0
    for element in crf['formElements']:
        question_text = element['label']
        crf_text += question_text
        count_elements += 1

        if 'question' in element and 'cde' in element['question']:
            crf_text += f" (name: {element['question']['cde']['name']})"
            count_cdes += 1

            if 'newCde' in element['question']['cde']:
                definitions = element['question']['cde']['newCde'].get('definitions') or []
                for definition in definitions:
                    if 'sources' in definition:
                        crf_text += f" (definition: {definition['definition']}, sources: {'; '.join(definition['sources'])})"
                    else:
                        crf_text += f" (definition: {definition['definition']})"

        crf_text += "\n"

    if add_cde_count_to_description:
        if len(description) == 0:
            description = f"Contains {count_cdes} CDEs."
        else:
            description = f"{description} Contains {count_cdes} CDEs."

    graph.add_node(crf_id)
    graph.add_node_attribute(crf_id, 'provided_by', source)
    graph.add_node_attribute(crf_id, 'name', crf_name)
    graph.add_node_attribute(crf_id, 'summary', description)
    graph.add_node_attribute(crf_id, 'cde_count', count_cdes)
    graph.add_node_attribute(crf_id, 'category', ['biolink:Publication'])
    # graph.add_node_attribute(crf_id, 'summary', crf_text)

    # Let's figure out how to categorize this CDE. We'll record two categories:
    # - 1. Let's create a `cde_categories` attribute that will be a list of all the categories
    #   we know about. This is the most comprehensive option, but is also likely to lead to
    #   incomplete categories such as "English", "Adult" and so on.
    file_paths = filter(lambda d: d['designation'].startswith('File path: '), crf['designations'])
    # chain.from_iterable() effectively flattens the list.
    categories = crf['categories'] # list(chain.from_iterable(map(lambda d: d['designation'][11:].split('/'), file_paths)))
    logging.info(f"Categories for CDE {crf_name}: {categories}")
    graph.add_node_attribute(crf_id, 'cde_categories', list(categories))
    # - 2. Let's create a `cde_category` property that summarizes the longlist of categories into
    #   the categories created in https://www.jpain.org/article/S1526-5900(21)00321-7/fulltext#tbl0001

    # The top-level category should tell us if it's core or not.
    core_or_not = categories[0]

    # Is this adult or pediatric?
    flag_has_adult_pediatric = False
    if 'Adult' in categories:
        adult_or_pediatric = 'Adult'
        flag_has_adult_pediatric = True
    elif 'Pediatric' in categories:
        adult_or_pediatric = 'Pediatric'
        flag_has_adult_pediatric = True
    else:
        adult_or_pediatric = 'Adult/Pediatric'
        logging.error(f"Could not determine if adult or pediatric from categories: {categories}")

    # Is this relating to acute or chronic pain?
    flag_has_acute_chronic = False
    if 'Acute Pain' in categories:
        acute_or_chronic_pain = 'Acute Pain'
        flag_has_acute_chronic = True
    elif 'Chronic Pain' in categories:
        acute_or_chronic_pain = 'Chronic Pain'
        flag_has_acute_chronic = True
    else:
        acute_or_chronic_pain = 'Acute/Chronic Pain'
        logging.error(f"Could not determine if acute or chronic pain from categories: {categories}")

    # Filter out any final categories that aren't the most specific category.
    if categories[-1] == 'English' or categories[-1] == 'Spanish':
        # We're not interested in these.
        del categories[-1]

    # The last category should now be the most specific category.
    # graph.add_node_attribute(crf_id, 'cde_category_extended', [
    #     core_or_not,
    #     adult_or_pediatric,
    #     acute_or_chronic_pain,
    #     categories[-1]
    # ])

    # Let's summarize all of this into a single category (as per
    # https://github.com/helxplatform/development/issues/868#issuecomment-1072485659)
    if flag_has_adult_pediatric:
        if flag_has_acute_chronic:
            cde_category = f"{acute_or_chronic_pain} ({adult_or_pediatric})"
        else:
            cde_category = adult_or_pediatric
    else:
        if flag_has_acute_chronic:
            cde_category = acute_or_chronic_pain
        else:
            cde_category = core_or_not

    # graph.add_node_attribute(crf_id, 'cde_category', cde_category)
    # logging.info(f"Categorized CRF {crf_name} as {cde_category}")

    crf['_ner'] = {
        'bagel': {
            'crf_name': crf_name,
            'crf_text': crf_text,
            'tokens': {
                'could_not_normalize': [],
                'ignored': [],
                'normalized': []
            }
        }
    }

    comprehensive = ner_via_bagel(crf_id, crf_text, sapbert_score_threshold=sapbert_score_threshold)
    crf['_ner']['bagel']['results'] = comprehensive
    crf['denotations'] = comprehensive.get('denotations', [])

    logging.info(f"Querying CRF '{designation}' with text: {crf_text} (CRF ID {crf_id})")
    existing_term_ids = set()
    for denotation in comprehensive.get('denotations', []):
        logging.info(f"Found denotation: {denotation}")
        count_tokens += 1

        concept = denotation.get('concept', {})

        if graph and concept:
            # Create the NamedThing that is the denotation.
            if 'id' in concept:
                term_id = concept['id']
            else:
                global count_errors
                count_errors += 1

                term_id = f'ERROR:{count_errors}'

            if term_id in IGNORED_CONCEPTS:
                global ignored_count
                ignored_count += 1

                logging.info(f'Ignoring concept {term_id} as it is on the list of ignored concepts')
                crf['_ner']['bagel']['tokens']['ignored'].append(denotation)
                count_ignored += 1
                continue

            crf['_ner']['bagel']['tokens']['normalized'].append(denotation)
            count_normalized += 1

            if term_id in existing_term_ids:
                # Suppress duplicate IDs to save space.
                continue
            existing_term_ids.add(term_id)

            edge_source = f'Bagel (with NameRes and SAPBERT with threshold = {sapbert_score_threshold})'

            graph.add_node(term_id)
            graph.add_node_attribute(term_id, 'category', concept['biolink_type'])
            graph.add_node_attribute(term_id, 'name', concept.get('label', ''))
            graph.add_node_attribute(term_id, 'provided_by', edge_source)

            # Add an edge/association between the CRF and the term.
            global association_count
            association_count += 1

            association_id = f'HEALCDE:edge_{association_count}'

            graph.add_edge(crf_id, term_id, association_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'category', ['biolink:InformationContentEntityToNamedThingAssociation'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'name', denotation['text'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'knowledge_source', edge_source)
            # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')
            # graph.add_edge_attribute(crf_id, term_id, association_id, 'description', f"NER found '{denotation['text']}' in CRF text '{crf_text}'")

            graph.add_edge_attribute(crf_id, term_id, association_id, 'subject', crf_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate', 'biolink:mentions') # https://biolink.github.io/biolink-model/docs/mentions.html
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate_label', 'mentions')

            graph.add_edge_attribute(crf_id, term_id, association_id, 'object', term_id)
        else:
            crf['_ner']['bagel']['tokens']['could_not_normalize'].append(denotation)
            count_could_not_normalize += 1

    return crf
