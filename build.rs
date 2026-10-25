use capnpc::CompilerCommand;

fn main() {
    CompilerCommand::new()
        .file("capnp/finder.capnp")
        .file("capnp/pages.capnp")
        .run()
        .expect("compiling schema");
    println!("cargo:rerun-if-changed=capnp/finder.capnp");
    println!("cargo:rerun-if-changed=capnp/pages.capnp");
}
