select
  a.STAT as "A_STAT",
  a.BIRT as "A_BIRT",
  a.ITNO as "A_ITNO",
  a.SERN as "A_SERN",
  a.ALII as "A_ALII",
  a.EQTP as "A_EQTP",
  b.WHLO as "B_WHLO",
  b.WHSL as "B_WHSL",
  b.FACI as "B_FACI",
  c.CFGL as "C_CFGL",
  c.MTRL as "C_MTRL",
  c.SERN as "C_SERN",
  d.HISN as "W_SERN",
  d.HIIT as "W_ITNO",
  w.STAT as "W_STAT",
  d.INDA as "RGDT",
  d.RGTM as "RGTM"
from MILOIN a
left outer join MITLOC b
  on a.CONO = b.CONO
  and a.ITNO = b.ITNO
  and a.SERN = b.BANO
left outer join MROICL c
  on c.CONO = a.CONO
  and a.ITNO = c.ITNO
  and a.SERN = c.SER2
left outer join MROUHI d
  on d.CONO = a.CONO
  and d.ITNO = a.ITNO
  and d.SERN = a.SERN
  and d.REMD = 0
  and d.REDN = 0
  and d.RSCD = ''
left outer join MILOIN w
  on w.CONO = d.CONO
  and w.ITNO = d.HIIT
  and w.SERN = d.HISN
where a.STAT < 99
  and a.EQTP <> '100'
  and a.CONO = '881'
