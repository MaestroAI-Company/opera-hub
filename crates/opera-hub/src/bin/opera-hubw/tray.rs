use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use opera_hub::config::Config;
use opera_hub::hub;
use opera_hub::store::Store;
use tao::dpi::{LogicalSize, PhysicalPosition};
use tao::event::{ElementState, Event, StartCause, WindowEvent};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopWindowTarget};
use tao::keyboard::Key;
use tao::window::{Window, WindowBuilder};
use tokio::sync::oneshot;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

const REFRESH: Duration = Duration::from_secs(5);
const OPERA_RED: [u8; 3] = [0xFF, 0x1A, 0x1A];
const QR_PANEL_SIZE: f64 = 280.0;
//space between the tray icon and the panel
const QR_PANEL_GAP: f64 = 8.0;
//white border scanners need around the code
const QR_QUIET_MODULES: usize = 4;

struct Labels {
    pair: &'static str,
    scan: &'static str,
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
            scan: "Scannez avec Opera · valable 10 min",
            quit: "Quitter",
            none: "Aucun appareil appairé",
            one: "1 appareil appairé",
            many: "appareils appairés",
        }
    } else {
        Labels {
            pair: "Pair a device",
            scan: "Scan with Opera · valid 10 min",
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

struct PairingWindow {
    window: Rc<Window>,
    surface: softbuffer::Surface<Rc<Window>, Rc<Window>>,
    dark: Vec<bool>,
    modules: usize,
}

//each click makes a new one time code
fn open_pairing(
    target: &EventLoopWindowTarget<UserEvent>,
    data_dir: &Path,
    labels: &Labels,
    anchor: Option<tray_icon::Rect>,
) -> Result<PairingWindow> {
    let link = hub::pairing_link(data_dir)?.to_string();
    let code = qrcode::QrCode::new(link.as_bytes())?;
    let dark = code
        .to_colors()
        .iter()
        .map(|c| *c == qrcode::Color::Dark)
        .collect();
    let builder = WindowBuilder::new()
        .with_title(labels.scan)
        .with_inner_size(LogicalSize::new(QR_PANEL_SIZE, QR_PANEL_SIZE))
        .with_resizable(false)
        .with_decorations(false)
        .with_always_on_top(true)
        .with_visible(false);
    #[cfg(windows)]
    let builder = {
        use tao::platform::windows::WindowBuilderExtWindows;
        builder.with_skip_taskbar(true)
    };
    let window = Rc::new(builder.build(target)?);
    if let Some(anchor) = anchor {
        place_next_to(&window, anchor);
    }
    window.set_visible(true);
    let context = softbuffer::Context::new(window.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    let surface =
        softbuffer::Surface::new(&context, window.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    window.set_focus();
    window.request_redraw();
    Ok(PairingWindow {
        window,
        surface,
        dark,
        modules: code.width(),
    })
}

//above a bottom taskbar, below a top menu bar
fn place_next_to(window: &Window, anchor: tray_icon::Rect) {
    let (ax, ay) = (anchor.position.x, anchor.position.y);
    let (aw, ah) = (anchor.size.width as f64, anchor.size.height as f64);
    let Some(monitor) = window
        .available_monitors()
        .find(|m| {
            let (p, s) = (m.position(), m.size());
            ax >= p.x as f64
                && ax < p.x as f64 + s.width as f64
                && ay >= p.y as f64
                && ay < p.y as f64 + s.height as f64
        })
        .or_else(|| window.current_monitor())
    else {
        return;
    };
    let (screen, screen_size) = (monitor.position(), monitor.size());
    let size = window.outer_size();
    let (w, h) = (size.width as f64, size.height as f64);
    let gap = QR_PANEL_GAP * window.scale_factor();
    let left = screen.x as f64;
    let right = (left + screen_size.width as f64 - w).max(left);
    let x = (ax + aw / 2.0 - w / 2.0).clamp(left, right);
    let middle = screen.y as f64 + screen_size.height as f64 / 2.0;
    let y = if ay < middle {
        ay + ah + gap
    } else {
        ay - h - gap
    };
    window.set_outer_position(PhysicalPosition::new(x, y));
}

fn draw_pairing(pairing: &mut PairingWindow) -> Result<()> {
    let size = pairing.window.inner_size();
    let (Some(width), Some(height)) = (NonZeroU32::new(size.width), NonZeroU32::new(size.height))
    else {
        return Ok(());
    };
    pairing
        .surface
        .resize(width, height)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut buffer = pairing
        .surface
        .buffer_mut()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let (width, height) = (size.width as usize, size.height as usize);
    let total = pairing.modules + QR_QUIET_MODULES * 2;
    let scale = (width.min(height) / total).max(1);
    let left = (width.saturating_sub(total * scale)) / 2;
    let top = (height.saturating_sub(total * scale)) / 2;
    for y in 0..height {
        for x in 0..width {
            let module = |pixel: usize, start: usize| {
                (pixel.checked_sub(start)? / scale).checked_sub(QR_QUIET_MODULES)
            };
            let dark = match (module(x, left), module(y, top)) {
                (Some(mx), Some(my)) if mx < pairing.modules && my < pairing.modules => {
                    pairing.dark[my * pairing.modules + mx]
                }
                _ => false,
            };
            buffer[y * width + x] = if dark { 0x000000 } else { 0xFFFFFF };
        }
    }
    buffer.present().map_err(|e| anyhow::anyhow!("{e}"))?;
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
    let mut pairing: Option<PairingWindow> = None;
    let mut stop = Some(stop);
    let mut hub_thread = Some(hub_thread);
    event_loop.run(move |event, target, control_flow| {
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
                let anchor = tray.as_ref().and_then(|t| t.rect());
                match open_pairing(target, &data_dir, &labels, anchor) {
                    Ok(window) => pairing = Some(window),
                    Err(e) => tracing::error!("cannot show the pairing code: {e:#}"),
                }
            }
            Event::RedrawRequested(id) => {
                if let Some(window) = pairing.as_mut().filter(|p| p.window.id() == id)
                    && let Err(e) = draw_pairing(window)
                {
                    tracing::error!("cannot draw the pairing code: {e:#}");
                }
            }
            //a click elsewhere or escape closes the panel
            Event::WindowEvent {
                event: WindowEvent::Focused(false) | WindowEvent::CloseRequested,
                ..
            } => pairing = None,
            Event::WindowEvent {
                event: WindowEvent::KeyboardInput { event, .. },
                ..
            } if event.logical_key == Key::Escape && event.state == ElementState::Pressed => {
                pairing = None
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
