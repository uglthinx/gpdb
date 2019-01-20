-- This test focus on partition table.
-- DMLs can operate on root or directly on the leaf.
-- Such DMLs on the same partition group might modify
-- the same table even their result relation is not
-- same. So to serialize these DMLs, their root relid
-- is taken into consideration. This test scripts is
-- `test: concurrent_dmls`. The difference is that
-- this concurrent transactions in this test, one is
-- on root table, the other is on leaf table.

-- Test two operations on same table should be serialized on QD: normal update, split-update
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: update tpart_concurrent_dmls set c2 = 999 where c2 = 1;
2&: update tpart_concurrent_dmls_1_prt_2 set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table should be serialized on QD: normal update, update whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update tpart_concurrent_dmls set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2&: update tpart_concurrent_dmls_1_prt_2 set c2 = 999 where c2 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: normal update, delete whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2&: update tpart_concurrent_dmls_1_prt_2 set c2 = 999 where c2 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: normal delete, split-update
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls where c2 = 1;
2&: update tpart_concurrent_dmls_1_prt_2 set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table should be serialized on QD: split-update, split-update
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: update tpart_concurrent_dmls set c1 = 888 where c1 = 1;
2&: update tpart_concurrent_dmls_1_prt_2 set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table should be serialized on QD: split-update, update whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update tpart_concurrent_dmls set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2&: update tpart_concurrent_dmls_1_prt_2 set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: split-update, delete whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2&: update tpart_concurrent_dmls_1_prt_2 set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: update whose plan contains motion, update whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update tpart_concurrent_dmls set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2&: update tpart_concurrent_dmls_1_prt_2 set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 = tpart_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: update whose plan contains motion, delete whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2&: update tpart_concurrent_dmls_1_prt_2 set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 = tpart_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2<:
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal update, normal delete
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls where c2 = 1;
2: update tpart_concurrent_dmls_1_prt_2 set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal update, insert
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 5)i;
2: update tpart_concurrent_dmls_1_prt_2 set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal update, normal update
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: update tpart_concurrent_dmls set c2 = 999 where c2 = 1;
2: update tpart_concurrent_dmls_1_prt_2 set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal delete, normal delete
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls where c2 = 1;
2: delete from tpart_concurrent_dmls_1_prt_2 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal delete, update whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls where c2 = 8;
2: update tpart_concurrent_dmls_1_prt_2 set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 = tpart_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal delete, insert
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls where c2 = 1;
2: insert into tpart_concurrent_dmls_1_prt_2 values (1,1,1);

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal delete, delete whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls where c2 = 1;
2: delete from tpart_concurrent_dmls_1_prt_2 using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 > tpart_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: split-update, insert
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 5)i;
2: update tpart_concurrent_dmls_1_prt_2 set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: update whose plan contains motion, insert
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 5)i;
2: update tpart_concurrent_dmls_1_prt_2 set c2 = 777 from tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 = tpart_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: delete whose plan contains motion, insert
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 5)i;
2: delete from tpart_concurrent_dmls_1_prt_2 using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 > tpart_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: delete whose plan contains motion, delete whose plan contains motion
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
create table tpart_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into tpart_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from tpart_concurrent_dmls using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls.c3 = tpart_concurrent_dmls_auxiliary.c3;
2: delete from tpart_concurrent_dmls_1_prt_2 using tpart_concurrent_dmls_auxiliary where tpart_concurrent_dmls_1_prt_2.c3 > tpart_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;
select * from tpart_concurrent_dmls_auxiliary;

drop table tpart_concurrent_dmls;
drop table tpart_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: insert, insert
create table tpart_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1) partition by range (c3) ( start (1) end (5) every (1), default partition extra );
insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: insert into tpart_concurrent_dmls select i,i,i from generate_series(1, 5)i;
2: insert into tpart_concurrent_dmls_1_prt_2 values (1,1,1);

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 'tpart_%';

1: end;
2: end;

1q:
2q:

select * from tpart_concurrent_dmls;

drop table tpart_concurrent_dmls;

