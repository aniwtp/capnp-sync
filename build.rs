use capnpc::CompilerCommand;

fn main() {
    CompilerCommand::new()
        .file("capnp/finder.capnp")
        .run()
        .expect("compiling schema");
    println!("cargo:rerun-if-changed=capnp/finder.capnp");
}
