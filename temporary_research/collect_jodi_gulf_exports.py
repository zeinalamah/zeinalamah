#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import requests

OUT=Path('research_output/paper2_v7_jodi'); OUT.mkdir(parents=True,exist_ok=True)
URL='https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/primary/primaryyear{year}.csv'
YEARS=range(2015,2027)
GULF={'SA','AE','QA','KW','BH','IQ','IR','OM'}
flows={'TOTEXPSB','TOTIMPSB','CLOSTLV','PROD','TOTDEMO','REFINOBS','STOCKCH'}
products={'CRUDEOIL','TOTCRUDE'}
frames=[]; logs=[]
s=requests.Session(); s.headers.update({'User-Agent':'academic-energy-chokepoint-research/1.0'})
for y in YEARS:
    url=URL.format(year=y)
    try:
        r=s.get(url,timeout=240); r.raise_for_status()
        path=OUT/f'primary_{y}.csv'; path.write_bytes(r.content)
        raw=pd.read_csv(path,dtype=str)
        required={'REF_AREA','TIME_PERIOD','ENERGY_PRODUCT','FLOW_BREAKDOWN','UNIT_MEASURE','OBS_VALUE','ASSESSMENT_CODE'}
        if not required.issubset(raw.columns): raise RuntimeError(f'columns {raw.columns.tolist()}')
        x=raw[raw.REF_AREA.isin(GULF)&raw.ENERGY_PRODUCT.isin(products)&raw.FLOW_BREAKDOWN.isin(flows)].copy()
        x['value']=pd.to_numeric(x.OBS_VALUE,errors='coerce'); x['month']=pd.to_datetime(x.TIME_PERIOD,errors='coerce'); x['source_year']=y
        frames.append(x[['REF_AREA','month','ENERGY_PRODUCT','FLOW_BREAKDOWN','UNIT_MEASURE','value','ASSESSMENT_CODE','source_year']])
        logs.append({'year':y,'status':'ok','bytes':len(r.content),'raw_rows':len(raw),'selected_rows':len(x)})
        path.unlink(missing_ok=True)
    except Exception as e:
        logs.append({'year':y,'status':'failed','error':repr(e)})
    print(logs[-1],flush=True)
pd.DataFrame(logs).to_csv(OUT/'jodi_download_log.csv',index=False)
if not frames: raise RuntimeError('No JODI data')
d=pd.concat(frames,ignore_index=True).sort_values(['REF_AREA','month','ENERGY_PRODUCT','FLOW_BREAKDOWN','source_year']).drop_duplicates(['REF_AREA','month','ENERGY_PRODUCT','FLOW_BREAKDOWN'],keep='last')
d.to_csv(OUT/'jodi_gulf_primary_monthly_2015_2026.csv.gz',index=False,compression='gzip')
w=d.pivot_table(index=['REF_AREA','month','ENERGY_PRODUCT'],columns='FLOW_BREAKDOWN',values='value',aggfunc='first').reset_index().rename_axis(columns=None)
w.to_csv(OUT/'jodi_gulf_primary_monthly_wide_2015_2026.csv.gz',index=False,compression='gzip')
summary={'rows_long':len(d),'rows_wide':len(w),'countries':sorted(d.REF_AREA.unique().tolist()),'first_month':str(d.month.min().date()),'latest_month':str(d.month.max().date()),'successful_years':int((pd.DataFrame(logs).status=='ok').sum()),'failed_years':int((pd.DataFrame(logs).status!='ok').sum())}
(OUT/'jodi_summary.json').write_text(json.dumps(summary,indent=2)); print(json.dumps(summary,indent=2),flush=True)
