set allow_system_table_mods = on;
create type pg_catalog.special_agg_state as
(
    first_row_val  float8,
    second_row_val float8,
    proportion     float8,
    cnt            int
);

create function pg_catalog.special_agg_trans(s special_agg_state, a float8, percent float8, total_rows bigint)
returns special_agg_state as
$$
declare
  first_row_id int;
  second_row_id int;
  cnt           int;
  proportion    float8;
  first_row_val float8;
  second_row_val float8;
begin
  cnt := s.cnt + 1;
  first_row_id := 1 + floor(percent * (total_rows - 1));
  second_row_id := 1 + ceil(percent * (total_rows - 1));
  proportion := (percent * (total_rows - 1)) - floor(percent * (total_rows - 1));
  if cnt = first_row_id then
      first_row_val := a;
  else
      first_row_val := s.first_row_val;
  end if;
  if cnt = second_row_id then
      second_row_val := a;
  else
       second_row_val := s.second_row_val;
  end if;
  return (first_row_val, second_row_val, proportion, cnt);
end;
$$ language plpgsql;

create function pg_catalog.special_agg_final(s special_agg_state) returns float8 as
$$
begin
  if s.proportion > 0 then
      return s.first_row_val + (s.proportion*(s.second_row_val - s.first_row_val));
  else
      return s.first_row_val;
  end if;
end;
$$ language plpgsql;

create AGGREGATE pg_catalog.special_agg_cont(float8, float8, bigint)
(
    sfunc = special_agg_trans,
    stype = special_agg_state,
    finalfunc = special_agg_final,
    initcond = '(-1, -1, 0, 0)'
);

create table t(a float8, b float8, c float8,
               d float8, e float8, f float8, g float8);
insert into t select i,i,i,i,i,i,i from generate_series(1, 10)i;
