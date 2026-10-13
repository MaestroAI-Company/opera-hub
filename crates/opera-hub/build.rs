//icon and company shown in the windows file properties
fn main() {
    #[cfg(windows)]
    {
        let mut resource = tauri_winres::WindowsResource::new();
        resource
            .set_icon("assets/icons/icon.ico")
            .set("ProductName", "Opera Hub")
            .set("FileDescription", "Opera Hub")
            .set("CompanyName", "Maestro AI")
            .set("LegalCopyright", "© Maestro AI");
        resource.compile().expect("windows resources");
    }
}
