create table Synthetic_Data (
  id	      bigserial PRIMARY KEY,
  join_key    bigint not null,
  property_1  bigint default 0,  
  property_2  bigint default 0,
  property_3  bigint default 0,
  property_4  bigint default 0,
  property_5  bigint default 0,
  property_6  bigint default 0,
  property_7  bigint default 0,
  property_8  bigint default 0,
  property_9  bigint default 0,
  property_10 bigint default 0
);