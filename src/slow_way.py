'''
This is the slow way... it is not able to multithread because of how the user page is returned.
'''

from phish_el.archive_reference.phish_elt_slow import run_pipeline
run_pipeline()