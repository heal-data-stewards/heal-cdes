#!/usr/bin/env python3
#
# id-lookups.py <directory containing HEAL CDEs as JSON files>
#
# HEAL CDEs have external identifiers that point to "CDISC" entities via NCIt identifiers.
# It would be great to use this to enrich the data that we have, but most of the referenced NCIt
# identifiers lack any useful metadata: they only point to a single concept unconnected from any
# other.
#
# For the moment, this script's sole function is to confirm this by using the NCI Thesaurus API
# (i.e. the LexEVS API, e.g. https://lexevscts2.nci.nih.gov/lexevscts2/codesystem/NCI_Thesaurus/entity/C33999)
# to retrieve everything known about that concept in both the NCI Thesaurus (if possible) in the
# NCI Metathesaurus.
