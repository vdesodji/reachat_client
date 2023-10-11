import pandas as pd
import connexion
from datetime import date
import numpy as np
import multiprocessing
import time

def stats_fid_2015_2019(year, base_test):
    
    base_test["datefacture"] = pd.to_datetime(base_test["datefacture"])
    list_clients = base_test.loc[(base_test["datefacture"].between(str(year)+'-01-01',
                                                                 str(year)+'-12-31'))]["id_client"].unique().tolist()
    stats = pd.DataFrame()
    nb_clients = len(list_clients)
    print(f''' Le nombre de client de départ est {len(list_clients)}''')
    
    if nb_clients > 0:
        for i in range(1,7):
            nb_clients_reachats = base_test.loc[(base_test["datefacture"]>str(year)+'-12-31')&
                                ((base_test["datefacture"]<str(year+i+1)+'-01-01'))&
                                (base_test["id_client"].isin(list_clients))]["id_client"].nunique()
            print(f''' Le nombre de client de réachat est {nb_clients_reachats}''')

            temp_data = pd.DataFrame({"Periode":["tx_reachat_entre_1_et"+str(i)+"_ans"],
                                    "tx_reachat":[nb_clients_reachats/nb_clients]})
            stats = pd.concat([stats, temp_data])
    return stats

def resultats(data, mags):
    root = "D:/DATA/CRM PROJECT/FID CLIENTS/RESULTS/REACHAT/VERSION_COSIUM/"
    all_stats = None
    for year in range(2019, 2023):
        try:
            stats = stats_fid_2015_2019(year, data)
            stats["annee_clients"]= year
            all_stats = pd.concat([all_stats, stats])
        except:
            print(f'''  il y a une erreur pour le magasin {mags} sur l'année {year}''')
    try:
        all_stats.to_parquet(root+data["code_magasin"][0]+".parquet")
    except:
        pass


if __name__ == '__main__':

    jobs = []

    conn, cur = connexion.Connexions("datamart_reseau").run()
    facture_code_mag = pd.read_sql(f''' select code_application,id_facture,
                                        code_magasin
                                    from flux_stats_ventes.bi_selecteur_flux_stats_ventes
                                    where grp_client = 'Z3' ''', conn)

    facture = pd.read_sql(f'''  select distinct code_application, id_facture, id_client, datefacture
                                from flux_stats_ventes.bi_articlefacture 
                                where produit_audio = 0
                                and numfacture not like 'R%'
                                and typearticle in ('VER', 'MON')
                                and rg_equipement = 1
                            ''', conn) 
    conn.close()
    invoice = facture.merge(facture_code_mag, on = (["code_application", "id_facture"]))
    
    conn, cur = connexion.Connexions("prisme").run()
    mags_actif = pd.read_sql('''select codemagasinsap from PRISME.VIEW_MAGASIN_ACTIF where grp_client = 'Z3' ''', conn)
    conn.close()
    invoice = invoice.loc[invoice["code_magasin"].isin(mags_actif["CODEMAGASINSAP"].tolist())].reset_index(drop = True)
    all_mags = invoice["code_magasin"].unique().tolist()
    # for  id_process in range(len(all_mags)):
    for  id_process, code_mag in enumerate(all_mags):
        base_test = invoice.loc[invoice["code_magasin"]== code_mag].reset_index(drop = True)
       
        p = multiprocessing.Process(target=resultats, args=(base_test,all_mags[id_process]))

        time.sleep(0.5)
        jobs.append(p)
        print('Starting process', id_process+1)
        p.start()

    for job in jobs:
        job.join()

