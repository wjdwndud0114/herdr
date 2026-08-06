use serde::Serialize;

use crate::fleet::RemoteInstance;

#[derive(Serialize)]
struct RemoteList<'a> {
    path: String,
    remotes: &'a [RemoteInstance],
}

pub(super) fn run_remote_command(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(String::as_str) {
        Some("list") => list(&args[1..]),
        Some("add") => add(&args[1..]),
        Some("remove") => remove(&args[1..]),
        Some("enable") => set_enabled(&args[1..], true),
        Some("disable") => set_enabled(&args[1..], false),
        Some("help" | "--help" | "-h") => {
            print_help();
            Ok(0)
        }
        _ => {
            print_help();
            Ok(2)
        }
    }
}

fn list(args: &[String]) -> std::io::Result<i32> {
    let json = match args {
        [] => false,
        [flag] if flag == "--json" => true,
        _ => {
            eprintln!("usage: herdr remote list [--json]");
            return Ok(2);
        }
    };
    let registry = crate::fleet::try_load()?;
    if json {
        let output = RemoteList {
            path: crate::fleet::registry_path().display().to_string(),
            remotes: &registry.instances,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(0);
    }
    if registry.instances.is_empty() {
        println!("No remote instances configured.");
        return Ok(0);
    }
    for instance in registry.instances {
        let state = if instance.enabled {
            "enabled"
        } else {
            "disabled"
        };
        let session = instance.session.as_deref().unwrap_or("default");
        println!(
            "{}\t{}\t{}\t{}\t{}",
            instance.id.as_str(),
            state,
            instance.name,
            instance.target,
            session
        );
    }
    Ok(0)
}

fn add(args: &[String]) -> std::io::Result<i32> {
    let Some(target) = args.first().filter(|arg| !arg.starts_with('-')) else {
        eprintln!("usage: herdr remote add <SSH_TARGET> [--name NAME] [--session NAME]");
        return Ok(2);
    };
    let mut name = None;
    let mut session = None;
    let mut index = 1;
    while index < args.len() {
        let flag = args[index].as_str();
        let value = args.get(index + 1).cloned();
        match (flag, value) {
            ("--name", Some(value)) if name.is_none() => name = Some(value),
            ("--session", Some(value)) if session.is_none() => session = Some(value),
            _ => {
                eprintln!("usage: herdr remote add <SSH_TARGET> [--name NAME] [--session NAME]");
                return Ok(2);
            }
        }
        index += 2;
    }

    let (instance, _) =
        crate::fleet::update(|registry| registry.add(target.clone(), name, session))?;
    println!(
        "Added {} ({}) as {}.",
        instance.name,
        instance.target,
        instance.id.as_str()
    );
    Ok(0)
}

fn remove(args: &[String]) -> std::io::Result<i32> {
    let [id] = args else {
        eprintln!("usage: herdr remote remove <REMOTE_ID>");
        return Ok(2);
    };
    let (instance, _) = crate::fleet::update(|registry| registry.remove(id))?;
    println!("Removed {} ({}).", instance.name, instance.id.as_str());
    Ok(0)
}

fn set_enabled(args: &[String], enabled: bool) -> std::io::Result<i32> {
    let [id] = args else {
        eprintln!(
            "usage: herdr remote {} <REMOTE_ID>",
            if enabled { "enable" } else { "disable" }
        );
        return Ok(2);
    };
    let (instance, _) = crate::fleet::update(|registry| registry.set_enabled(id, enabled))?;
    println!(
        "{} {} ({}).",
        if enabled { "Enabled" } else { "Disabled" },
        instance.name,
        instance.id.as_str()
    );
    Ok(0)
}

fn print_help() {
    eprintln!("herdr remote commands:");
    eprintln!("  herdr remote list [--json]");
    eprintln!("  herdr remote add <SSH_TARGET> [--name NAME] [--session NAME]");
    eprintln!("  herdr remote remove <REMOTE_ID>");
    eprintln!("  herdr remote enable <REMOTE_ID>");
    eprintln!("  herdr remote disable <REMOTE_ID>");
}
