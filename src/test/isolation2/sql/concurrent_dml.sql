-- This test script contains 21 blocks, it loops each possible sitution
-- for the concurrence of DMLs in Greenplum DB.

-- Some DMLs (like splitupdate, update/delete whose
-- plans contains Motion) cannot be concurrently executed.
-- This commit uses a new type of Lock to serialize these
-- DMLs execution on QD.

-- DMLs can be grouped into six class:
--   c1. normal update (update statement whose plan contains no motion)
--   c2. normal delete (delete statement whose plan contains no motion)
--   c3. split-update (update statement on hash cols of hash-distributed table)
--   c4. update whose plan has motions (like update t1 set c = c + 1 from t2 where ...)
--   c5. delete whose plan has motions (like delete from t1 using t2 where t1.c = t2.c)

-- If these operations are on the same table, the conflict relation among them
-- should be:

-- c1 conflict with [c3, c4, c5]
-- c2 conflict with [c3]
-- c3 conflict with [c1, c2, c3, c4, c5]
-- c4 conflict with [c1, c3, c4, c5]
-- c5 conflict with [c1, c3, c4]
-- So the lockmode for these five operations can be:
--   c1 ---- RowExclusiveLock
--   c2 ---- RowShareLock
--   c3 ---- ExclusiveLock
--   c4 ---- ShareRowExclusiveLock
--   c5 ---- ShareLock


-- Test two operations on same table should be serialized on QD: normal update, split-update
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: update t_concurrent_dmls set c2 = 999 where c2 = 1;
2&: update t_concurrent_dmls set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table should be serialized on QD: normal update, update whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2&: update t_concurrent_dmls set c2 = 999 where c2 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: normal update, delete whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2&: update t_concurrent_dmls set c2 = 999 where c2 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: normal delete, split-update
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls where c2 = 1;
2&: update t_concurrent_dmls set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table should be serialized on QD: split-update, split-update
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: update t_concurrent_dmls set c1 = 888 where c1 = 1;
2&: update t_concurrent_dmls set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table should be serialized on QD: split-update, update whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2&: update t_concurrent_dmls set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: split-update, delete whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2&: update t_concurrent_dmls set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: update whose plan contains motion, update whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2&: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table should be serialized on QD: update whose plan contains motion, delete whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2&: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2<:
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal update, normal update
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: update t_concurrent_dmls set c2 = 999 where c2 = 1;
2: update t_concurrent_dmls set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal update, insert
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: insert into t_concurrent_dmls select * from generate_series(1, 5);
2: update t_concurrent_dmls set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal update, normal delete
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls where c2 = 1;
2: update t_concurrent_dmls set c2 =666 where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal delete, normal delete
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls where c2 = 1;
2: delete from t_concurrent_dmls where c2 = 2;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal delete, insert
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls where c2 = 1;
2: insert into t_concurrent_dmls select * from generate_series(1, 5);

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: normal delete, delete whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls where c2 = 1;
2: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 > t_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: normal delete, update whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls where c2 = 9;
2: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: split-update, insert
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: insert into t_concurrent_dmls select * from generate_series(1, 5);
2: update t_concurrent_dmls set c1 = 888 where c1 = 1;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

-- Test two operations on same table can be concurrent on QD: update whose plan contains motion, insert
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: insert into t_concurrent_dmls select * from generate_series(1, 5);
2: update t_concurrent_dmls set c2 = 777 from t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: delete whose plan contains motion, delete whose plan contains motion
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 = t_concurrent_dmls_auxiliary.c3;
2: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 > t_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: delete whose plan contains motion, insert
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
create table t_concurrent_dmls_auxiliary(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;
insert into t_concurrent_dmls_auxiliary select i,i,i from generate_series(1, 2)i;

1: begin;
2: begin;

1: insert into t_concurrent_dmls select * from generate_series(1, 5);
2: delete from t_concurrent_dmls using t_concurrent_dmls_auxiliary where t_concurrent_dmls.c3 > t_concurrent_dmls_auxiliary.c3 + 10;

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;
select * from t_concurrent_dmls_auxiliary;

drop table t_concurrent_dmls;
drop table t_concurrent_dmls_auxiliary;

-- Test two operations on same table can be concurrent on QD: insert, insert
create table t_concurrent_dmls(c1 int, c2 int, c3 int) distributed by (c1);
insert into t_concurrent_dmls select i,i,i from generate_series(1, 20)i;

1: begin;
2: begin;

1: insert into t_concurrent_dmls select * from generate_series(1, 5);
2: insert into t_concurrent_dmls select * from generate_series(1, 5);

select * from (select gp_segment_id, locktype, relation::regclass::text, mode, granted from pg_locks where locktype = 'relation-dml')x
where relation like 't_concurrent_dmls%';

1: end;
2: end;

1q:
2q:

select * from t_concurrent_dmls;

drop table t_concurrent_dmls;

