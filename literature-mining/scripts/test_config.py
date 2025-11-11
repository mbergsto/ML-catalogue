from pybliometrics.scopus import AbstractRetrieval
import pybliometrics
pybliometrics.scopus.init()
d = AbstractRetrieval("2-s2.0-0034570550", id_type="eid", view="FULL", refresh=True)
print("Retrieval quota left:", d.get_key_remaining_quota())
