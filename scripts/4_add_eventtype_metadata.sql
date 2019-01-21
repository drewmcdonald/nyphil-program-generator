
.load data/event_types.txt etype_lookup

update eventtype set
  is_modelable = (select case when modelable='Y' then 1 else 0 end
                  from etype_lookup
                  where eventType = name)
where exists (select modelable
              from etype_lookup
              where eventType=name);

update eventtype set
  category = (select eventType_broad
              from etype_lookup
              where eventType = name)
where exists (select eventType_broad
              from etype_lookup
              where eventType=name);

drop table etype_lookup;
