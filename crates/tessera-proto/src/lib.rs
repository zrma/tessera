#![allow(clippy::all)]
#![allow(clippy::pedantic)]
#![allow(clippy::nursery)]
#![allow(clippy::restriction)]

pub mod orch {
    pub mod v1 {
        tonic::include_proto!("tessera.orch.v1");
    }
}
