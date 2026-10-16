//landlock limits files from the kernel side
use std::ffi::OsString;
use std::path::Path;

use anyhow::Result;
use landlock::{
    ABI, Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, path_beneath_rules,
};
use tokio::process::Command;

pub use super::unix::Process;
use super::{Child, Isolation, Policy};

//system files programs need to start
const SYSTEM_READ: [&str; 11] = [
    "/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc", "/proc", "/sys", "/dev", "/run", "/opt",
];
const SYSTEM_WRITE: [&str; 3] = ["/dev/null", "/dev/zero", "/dev/tty"];

pub fn isolation() -> Isolation {
    //asking for the abi version probes support
    let version = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0usize,
            1u32,
        )
    };
    if version >= 1 {
        Isolation::Full
    } else {
        Isolation::Partial
    }
}

pub fn forget(_name: &str) {}

pub fn spawn(
    program: &Path,
    args: &[OsString],
    env: &[(OsString, OsString)],
    cwd: &Path,
    policy: &Policy,
) -> Result<Child> {
    let abi = ABI::V5;
    //built here, applied in the child before exec
    let mut rules = Some(
        Ruleset::default()
            .handle_access(AccessFs::from_all(abi))?
            .create()?
            .add_rules(path_beneath_rules(SYSTEM_READ, AccessFs::from_read(abi)))?
            .add_rules(path_beneath_rules(&policy.shared, AccessFs::from_read(abi)))?
            .add_rules(path_beneath_rules(&policy.read, AccessFs::from_read(abi)))?
            .add_rules(path_beneath_rules(SYSTEM_WRITE, AccessFs::from_all(abi)))?
            .add_rules(path_beneath_rules(&policy.write, AccessFs::from_all(abi)))?,
    );
    let mut command = Command::new(program);
    command
        .args(args)
        .env_clear()
        .envs(env.iter().cloned())
        .current_dir(cwd);
    unsafe {
        command.pre_exec(move || {
            //dies with the hub
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            if let Some(rules) = rules.take() {
                rules
                    .restrict_self()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
            Ok(())
        });
    }
    super::unix::spawn(command)
}
