-- This test focus on inherit table.
-- DMLs can operate on root or directly on the leaf.
-- Such DMLs on the same partition group might modify
-- the same table even their result relation is not
-- same. So to serialize these DMLs, their root relid
-- is taken into consideration. This test scripts is
-- `test: concurrent_dmls`. The difference is that
-- this concurrent transactions in this test, one is
-- on root table, the other is on leaf table.

-- Test two operations on same table should be serialized on QD: normal update, split-update
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: update tinherit_concurrent_dmls_base set c2 = 999 where c2 = 1;
2&: update tinherit_concurrent_dmls_child set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table should be serialized on QD: normal update, update whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update tinherit_concurrent_dmls_base set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2&: update tinherit_concurrent_dmls_child set c2 = 999 where c2 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: normal update, delete whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2&: update tinherit_concurrent_dmls_child set c2 = 999 where c2 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: normal delete, split-update
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base where c2 = 1;
2&: update tinherit_concurrent_dmls_child set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table should be serialized on QD: split-update, split-update
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: update tinherit_concurrent_dmls_base set c1 = 888 where c1 = 1;
2&: update tinherit_concurrent_dmls_child set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table should be serialized on QD: split-update, update whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update tinherit_concurrent_dmls_base set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2&: update tinherit_concurrent_dmls_child set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: split-update, delete whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2&: update tinherit_concurrent_dmls_child set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: update whose plan contains motion, update whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update tinherit_concurrent_dmls_base set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2&: update tinherit_concurrent_dmls_child set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 = tinherit_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: update whose plan contains motion, delete whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2&: update tinherit_concurrent_dmls_child set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 = tinherit_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal update, normal delete
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base where c2 = 1;
2: update tinherit_concurrent_dmls_child set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table can be concurrent on QD: normal update, insert
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: insert into tinherit_concurrent_dmls_base select * from generate_series(1, 5);
2: update tinherit_concurrent_dmls_child set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table can be concurrent on QD: normal update, normal update
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: update tinherit_concurrent_dmls_base set c2 = 999 where c2 = 1;
2: update tinherit_concurrent_dmls_child set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table can be concurrent on QD: normal delete, update whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base where c2 = 1;
2: update tinherit_concurrent_dmls_child set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 = tinherit_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal delete, delete whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base where c2 = 1;
2: delete from tinherit_concurrent_dmls_child using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 > tinherit_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal delete, normal delete
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base where c2 = 1;
2: delete from tinherit_concurrent_dmls_child where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table can be concurrent on QD: normal delete, insert
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base where c2 = 1;
2: insert into tinherit_concurrent_dmls_child select * from generate_series(1, 5);

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table can be concurrent on QD: split-update, insert
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: insert into tinherit_concurrent_dmls_base select * from generate_series(1, 5);
2: update tinherit_concurrent_dmls_child set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

-- Test two operations on same table can be concurrent on QD: update whose plan contains motion, insert
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: insert into tinherit_concurrent_dmls_base select * from generate_series(1, 5);
2: update tinherit_concurrent_dmls_child set c2 = 777 from tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 = tinherit_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: delete whose plan contains motion, insert
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: insert into tinherit_concurrent_dmls_base select * from generate_series(1, 5);
2: delete from tinherit_concurrent_dmls_child using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 > tinherit_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: delete whose plan contains motion, delete whose plan contains motion
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
create table tinherit_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;
insert into tinherit_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tinherit_concurrent_dmls_base using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_base.c3 = tinherit_concurrent_dmls_auxiliary.c3;
2: delete from tinherit_concurrent_dmls_child using tinherit_concurrent_dmls_auxiliary where tinherit_concurrent_dmls_child.c3 > tinherit_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;

select * from tinherit_concurrent_dmls_auxiliary;

drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;
drop table tinherit_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: insert, insert
create table tinherit_concurrent_dmls_base(c1 int, c2 int, c3 int) distributed by (c1);
create table tinherit_concurrent_dmls_child(c4 int) inherits (tinherit_concurrent_dmls_base);
insert into tinherit_concurrent_dmls_base select i,i,i from generate_series(1, 10)i;
insert into tinherit_concurrent_dmls_child select i,i,i,i from generate_series(11, 15)i;

1: begin;
2: begin;

1: insert into tinherit_concurrent_dmls_base select * from generate_series(1, 5);
2: insert into tinherit_concurrent_dmls_child select * from generate_series(1, 5);

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x where relation like 'tinherit_concurrent_dmls%';
1: end;
2: end;

1q:
2q:

select * from tinherit_concurrent_dmls_base;
select * from tinherit_concurrent_dmls_child;


drop table tinherit_concurrent_dmls_child;
drop table tinherit_concurrent_dmls_base;

