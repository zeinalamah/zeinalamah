#!/usr/bin/env python3
from __future__ import annotations
import json, re, time
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
import requests
try:
    import searoute as sr
except Exception:
    sr=None
OUT=Path('research_output/paper2_v7'); OUT.mkdir(parents=True,exist_ok=True)
META='https://services9.arcgis.com/weJ1QsnbMYJlCHdG/ArcGIS/rest/services/PortWatch_ports_database/FeatureServer/0/query'
DAILY='https://services9.arcgis.com/weJ1QsnbMYJlCHdG/ArcGIS/rest/services/Daily_Ports_Data/FeatureServer/0/query'
BUNKER='https://agtransport.usda.gov/resource/4v3x-mj86.csv'
EU={'AUT','BEL','BGR','HRV','CYP','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','IRL','ITA','LVA','LTU','LUX','MLT','NLD','POL','PRT','ROU','SVK','SVN','ESP','SWE'}
ME={'SAU','ARE','OMN','QAT','KWT','BHR','IRQ','IRN','YEM','DJI','EGY','JOR','ISR'}
CONTROL={'NOR','GBR','USA','CAN','MEX','BRA','DZA','LBY','NGA','AGO','AZE','KAZ','RUS','TUR','IND','CHN','JPN','KOR','SGP','MYS','IDN','AUS'}
KEY=re.compile(r'yanbu|fujair|jebel ali|ruwais|ras tanura|jubail|dammam|jeddah|jizan|king abdullah|ras laffan|mesaieed|mina al ahmadi|mina saud|basra|umm qasr|khor al zubair|kharg|bandar imam|bandar abbas|jask|chabahar|sohar|duqm|salalah|rotterdam|antwerp|marseille|fos|trieste|genoa|piraeus|algeciras|sines|le havre|wilhelmshaven|gdansk|gothenburg|milford haven|teesside|grangemouth|skikda|arzew|bonny',re.I)
FIELDS='date,year,month,day,portid,portname,country,ISO3,portcalls_tanker,portcalls_cargo,portcalls,import_tanker,export_tanker,import_cargo,export_cargo,import,export'
def ses():
 s=requests.Session(); s.headers.update({'User-Agent':'academic-energy-chokepoint-research/1.0'}); return s
def jget(s,url,p,tries=8):
 for a in range(tries):
  try:
   r=s.get(url,params=p,timeout=180); r.raise_for_status(); x=r.json()
   if x.get('error'): raise RuntimeError(x['error'])
   return x
  except Exception:
   if a==tries-1: raise
   time.sleep(min(60,2**a))
def aq(s,url,where,fields,order=None,page=3000):
 total=int(jget(s,url,{'where':where,'returnCountOnly':'true','f':'json'}).get('count',0)); out=[]
 print({'records':total,'where':where[:70]},flush=True)
 for off in range(0,total,page):
  p={'where':where,'outFields':fields,'returnGeometry':'false','f':'json','resultOffset':off,'resultRecordCount':page}
  if order:p['orderByFields']=order
  fs=jget(s,url,p).get('features',[])
  if not fs:break
  out.append(pd.DataFrame([z.get('attributes',{}) for z in fs])); print({'offset':off,'received':len(fs)},flush=True)
 return pd.concat(out,ignore_index=True) if out else pd.DataFrame()
def classify(x):
 x=x.copy(); x['portid']=x.portid.astype(str); x['portname']=x.portname.fillna('').astype(str); x['ISO3']=x.ISO3.fillna('').str.upper(); n=x.portname.str.lower(); iso=x.ISO3
 x['hormuz_exposed_port']=0
 x.loc[iso.isin({'QAT','KWT','BHR','IRQ'}),'hormuz_exposed_port']=1
 x.loc[iso.eq('IRN')&~n.str.contains(r'jask|chabahar'),'hormuz_exposed_port']=1
 x.loc[iso.eq('ARE')&~n.str.contains(r'fujair|khor fakkan|kalba'),'hormuz_exposed_port']=1
 x.loc[iso.eq('SAU')&n.str.contains(r'ras tanura|jubail|dammam|khobar|khafji|ras al khair'),'hormuz_exposed_port']=1
 x['hormuz_bypass_port']=n.str.contains(r'fujair|yanbu|jask').astype('int8')
 x['saudi_gulf_port']=(iso.eq('SAU')&x.hormuz_exposed_port.eq(1)).astype('int8'); x['saudi_redsea_port']=(iso.eq('SAU')&n.str.contains(r'yanbu|jeddah|jizan|king abdullah|rabigh')).astype('int8')
 x['uae_gulf_port']=(iso.eq('ARE')&x.hormuz_exposed_port.eq(1)).astype('int8'); x['uae_fujairah_port']=(iso.eq('ARE')&n.str.contains(r'fujair|khor fakkan')).astype('int8')
 x['route_class']='other'; x.loc[x.hormuz_exposed_port.eq(1),'route_class']='inside_hormuz'; x.loc[x.hormuz_bypass_port.eq(1),'route_class']='hormuz_bypass'; x.loc[x.saudi_redsea_port.eq(1),'route_class']='saudi_red_sea'
 return x
def select(meta):
 x=classify(meta)
 for c in ['vessel_count_total','vessel_count_tanker','lat','lon']: x[c]=pd.to_numeric(x.get(c),errors='coerce')
 rel=x[x.ISO3.isin(EU|ME|CONTROL)]; top=rel.sort_values(['ISO3','vessel_count_tanker'],ascending=[True,False]).groupby('ISO3',group_keys=False).head(10); key=x[x.portname.str.contains(KEY,na=False)]; donors=x.sort_values('vessel_count_tanker',ascending=False).head(120)
 return classify(pd.concat([top,key,donors]).drop_duplicates('portid')).sort_values('vessel_count_tanker',ascending=False).head(420).reset_index(drop=True)
def daily_collect(s,ports):
 out=[]; ids=ports.portid.astype(str).tolist()
 for i in range(0,len(ids),12):
  g=[z.replace("'","''") for z in ids[i:i+12]]; w=' OR '.join([f"portid='{z}'" for z in g]); print({'batch':i//12+1,'ports':len(g)},flush=True); q=aq(s,DAILY,w,FIELDS,'date ASC,portid ASC')
  if len(q):out.append(q)
 x=pd.concat(out,ignore_index=True); x.portid=x.portid.astype(str); raw=pd.to_numeric(x.date,errors='coerce'); x['date']=pd.to_datetime(raw,unit='ms',errors='coerce',utc=True).dt.tz_localize(None)
 for c in ['portcalls_tanker','portcalls_cargo','portcalls','import_tanker','export_tanker','import_cargo','export_cargo','import','export']:x[c]=pd.to_numeric(x[c],errors='coerce').fillna(0)
 keep=['portid','lat','lon','LOCODE','route_class','hormuz_exposed_port','hormuz_bypass_port','saudi_gulf_port','saudi_redsea_port','uae_gulf_port','uae_fujairah_port']; x=x.merge(ports[keep],on='portid',how='left',validate='many_to_one'); x['week']=x.date.dt.to_period('W-MON').dt.start_time; x['month_date']=x.date.dt.to_period('M').dt.start_time
 return x.sort_values(['portid','date'])
def aggregates(d):
 f=['portcalls_tanker','portcalls_cargo','portcalls','import_tanker','export_tanker','import_cargo','export_cargo','import','export']
 return {'port_week':d.groupby(['week','portid','portname','ISO3','route_class','hormuz_exposed_port','hormuz_bypass_port'],as_index=False)[f].sum().rename(columns={'week':'date'}),'port_month':d.groupby(['month_date','portid','portname','ISO3','route_class','hormuz_exposed_port','hormuz_bypass_port'],as_index=False)[f].sum().rename(columns={'month_date':'month'}),'country_week':d.groupby(['week','ISO3','country'],as_index=False)[f].sum().rename(columns={'week':'date'}),'route_group_week':d.groupby(['week','route_class'],as_index=False)[f].sum().rename(columns={'week':'date'})}
def route(o,d,r):
 if sr is None:return np.nan
 try:return float(sr.searoute([float(o.lon),float(o.lat)],[float(d.lon),float(d.lat)],units='naut',speed_knot=12,restrictions=r,append_orig_dest=True).properties.get('length',np.nan))
 except Exception:return np.nan
def route_matrix(p,d):
 pre=d[d.date.between('2022-01-01','2023-09-30')]; oo=pre[pre.ISO3.isin(ME)].groupby('portid').export_tanker.sum().nlargest(25).index.tolist(); oo=list(dict.fromkeys(oo+p[p.portname.str.contains(KEY,na=False)&p.ISO3.isin(ME)].portid.tolist())); dd=pre[pre.ISO3.isin(EU)].groupby('portid').import_tanker.sum().nlargest(25).index.tolist(); m=p.set_index('portid'); out=[]
 for oi in oo:
  if oi not in m.index:continue
  o=m.loc[oi]
  for di in dd:
   if di not in m.index or di==oi:continue
   z=m.loc[di]; b=route(o,z,['northwest']); a=route(o,z,['northwest','suez','babalmandab']); h=route(o,z,['northwest','ormuz']); out.append({'origin_portid':oi,'origin_portname':o.portname,'origin_iso3':o.ISO3,'origin_route_class':o.route_class,'destination_portid':di,'destination_portname':z.portname,'destination_iso3':z.ISO3,'distance_nm_baseline':b,'distance_nm_avoid_red_sea':a,'distance_nm_avoid_hormuz':h,'redsea_distance_increase_pct':100*(a/b-1) if np.isfinite(b) and b>0 and np.isfinite(a) else np.nan,'redsea_cycle_capacity_ratio':b/a if np.isfinite(b) and b>0 and np.isfinite(a) and a>0 else np.nan,'hormuz_route_available':int(np.isfinite(h))})
 return pd.DataFrame(out)
def main():
 errs=[]
 with ses() as s:
  meta=aq(s,META,'1=1','*','ObjectId ASC'); meta.to_csv(OUT/'portwatch_ports_metadata_all.csv',index=False); ports=select(meta); ports.to_csv(OUT/'portwatch_selected_energy_ports.csv',index=False); print({'ports':len(ports),'countries':ports.ISO3.nunique()},flush=True); d=daily_collect(s,ports); d.to_csv(OUT/'portwatch_selected_ports_daily_2019_2026.csv.gz',index=False,compression='gzip')
  for n,x in aggregates(d).items():x.to_csv(OUT/f'portwatch_{n}_2019_2026.csv.gz',index=False,compression='gzip')
  try:
   r=s.get(BUNKER,params={'$limit':50000,'$order':'day ASC'},timeout=180); r.raise_for_status(); bp=pd.read_csv(pd.io.common.BytesIO(r.content)); bp['day']=pd.to_datetime(bp.day,errors='coerce',utc=True).dt.tz_localize(None); bp.to_csv(OUT/'usda_daily_bunker_fuel_prices.csv',index=False)
  except Exception as e:bp=pd.DataFrame(); errs.append({'component':'bunker','error':repr(e)})
 try:rm=route_matrix(ports,d); rm.to_csv(OUT/'energy_port_maritime_route_distance_matrix.csv.gz',index=False,compression='gzip')
 except Exception as e:rm=pd.DataFrame(); errs.append({'component':'routes','error':repr(e)})
 pd.DataFrame(errs).to_csv(OUT/'extension_errors.csv',index=False); s={'metadata_ports':len(meta),'selected_ports':len(ports),'selected_countries':ports.ISO3.nunique(),'daily_rows':len(d),'daily_ports':d.portid.nunique(),'first_date':str(d.date.min().date()),'latest_date':str(d.date.max().date()),'bunker_rows':len(bp),'route_pairs':len(rm),'errors':errs,'measurement_note':'PortWatch tanker imports and exports are AIS-based estimated metric-ton payload changes, not commodity-specific customs data.'}; (OUT/'port_delivery_extension_summary.json').write_text(json.dumps(s,indent=2,default=str)); print(json.dumps(s,indent=2,default=str),flush=True)
if __name__=='__main__':main()
