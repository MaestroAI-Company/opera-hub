use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use opera_hub::config::Config;
use opera_hub::hub;
use opera_hub::store::Store;
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder};
use tokio::sync::oneshot;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

const REFRESH: Duration = Duration::from_secs(5);
const OPERA_RED: [u8; 3] = [0xFF, 0x1A, 0x1A];

struct Labels {
    pair: &'static str,
    quit: &'static str,
    none: &'static str,
    one: &'static str,
    many: &'static str,
}

fn labels() -> Labels {
    let french = sys_locale::get_locale().is_some_and(|l| l.starts_with("fr"));
    if french {
        Labels {
            pair: "Appairer un appareil",
            quit: "Quitter",
            none: "Aucun appareil appairé",
            one: "1 appareil appairé",
            many: "appareils appairés",
        }
    } else {
        Labels {
            pair: "Pair a device",
            quit: "Quit",
            none: "No paired device",
            one: "1 paired device",
            many: "paired devices",
        }
    }
}

fn devices_label(labels: &Labels, data_dir: &Path) -> String {
    match Store::new(data_dir).devices().map(|d| d.len()).unwrap_or(0) {
        0 => labels.none.into(),
        1 => labels.one.into(),
        n => format!("{n} {}", labels.many),
    }
}

//template on macos follows the menu bar theme
fn icon() -> Result<Icon> {
    let decoder = png::Decoder::new(std::io::Cursor::new(include_bytes!(
        "../../../assets/tray.png"
    )));
    let mut reader = decoder.read_info()?;
    let mut rgba = vec![0; reader.output_buffer_size().context("icon too large")?];
    let info = reader.next_frame(&mut rgba)?;
    rgba.truncate(info.buffer_size());
    //black disappears on a dark windows taskbar
    if cfg!(windows) {
        for pixel in rgba.chunks_exact_mut(4) {
            pixel[..3].copy_from_slice(&OPERA_RED);
        }
    }
    Ok(Icon::from_rgba(rgba, info.width, info.height)?)
}

//the qr opens in the default image viewer
fn show_pairing(data_dir: &Path) -> Result<()> {
    let link = hub::pairing_link(data_dir)?.to_string();
    let svg = qrcode::QrCode::new(link.as_bytes())?
        .render::<qrcode::render::svg::Color>()
        .min_dimensions(320, 320)
        .build();
    let path = data_dir.join("pairing.svg");
    std::fs::write(&path, svg)?;
    #[cfg(windows)]
    Command::new("explorer").arg(&path).spawn()?;
    #[cfg(target_os = "macos")]
    Command::new("open").arg(&path).spawn()?;
    Ok(())
}

enum UserEvent {
    Menu(MenuEvent),
}

pub fn run(data_dir: PathBuf, stop: oneshot::Sender<()>, hub_thread: JoinHandle<()>) -> Result<()> {
    let labels = labels();
    let name = Config::load(&data_dir)?.hub_name();

    #[allow(unused_mut)]
    let mut event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    //menu bar only, no dock icon
    #[cfg(target_os = "macos")]
    {
        use tao::platform::macos::{ActivationPolicy, EventLoopExtMacOS};
        event_loop.set_activation_policy(ActivationPolicy::Accessory);
    }
    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(UserEvent::Menu(event));
    }));

    let title = MenuItem::new(format!("Opera Hub · {name}"), false, None);
    let devices = MenuItem::new(devices_label(&labels, &data_dir), false, None);
    let pair = MenuItem::new(labels.pair, true, None);
    let quit = MenuItem::new(labels.quit, true, None);
    let menu = Menu::new();
    menu.append_items(&[
        &title,
        &devices,
        &PredefinedMenuItem::separator(),
        &pair,
        &quit,
    ])?;
    let icon = icon()?;

    let mut tray: Option<TrayIcon> = None;
    let mut stop = Some(stop);
    let mut hub_thread = Some(hub_thread);
    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::WaitUntil(Instant::now() + REFRESH);
        match event {
            //macos wants the icon once the loop runs
            Event::NewEvents(StartCause::Init) => {
                let builder = TrayIconBuilder::new()
                    .with_menu(Box::new(menu.clone()))
                    .with_tooltip("Opera Hub");
                #[cfg(target_os = "macos")]
                let builder = builder.with_icon_templated(icon.clone());
                #[cfg(windows)]
                let builder = builder.with_icon(icon.clone());
                match builder.build() {
                    Ok(icon) => tray = Some(icon),
                    Err(e) => tracing::error!("cannot show the tray icon: {e}"),
                }
            }
            Event::NewEvents(StartCause::ResumeTimeReached { .. }) => {
                devices.set_text(devices_label(&labels, &data_dir));
            }
            Event::UserEvent(UserEvent::Menu(event)) if event.id == *pair.id() => {
                if let Err(e) = show_pairing(&data_dir) {
                    tracing::error!("cannot show the pairing code: {e:#}");
                }
            }
            Event::UserEvent(UserEvent::Menu(event)) if event.id == *quit.id() => {
                tray.take();
                if let Some(stop) = stop.take() {
                    let _ = stop.send(());
                }
                //lets the hub close its connections
                if let Some(thread) = hub_thread.take() {
                    let _ = thread.join();
                }
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    })
}
