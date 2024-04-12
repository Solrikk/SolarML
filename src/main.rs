use csv::WriterBuilder;
use reqwest;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use xml::reader::{EventReader, XmlEvent};

#[derive(Debug, Serialize)]
struct Item {
    id: String,
    category_name: String,
    #[serde(flatten)]
    extra: HashMap<String, String>,
    pictures: String,
}

fn fetch_and_parse_xml(url: &str) -> Result<Vec<Item>, Box<dyn Error>> {
    let response = reqwest::blocking::get(url)?.text()?;
    let mut parser = EventReader::from_str(&response);
    let mut items = Vec::new();
    let mut temp_item = Item {
        id: String::new(),
        category_name: String::new(),
        extra: HashMap::new(),
        pictures: String::new(),
    };
    let mut inside_offer = false;
    let mut current_element_name = String::new();
    let mut category_names = HashMap::new();

    while let Ok(e) = parser.next() {
        match e {
            XmlEvent::StartElement {
                name, attributes, ..
            } if name.local_name == "offer" => {
                inside_offer = true;
                temp_item.id = attributes
                    .iter()
                    .find(|a| a.name.local_name == "id")
                    .map_or(String::new(), |a| a.value.clone());
            }
            XmlEvent::StartElement {
                name, attributes, ..
            } if name.local_name == "category" && !inside_offer => {
                let id = attributes
                    .iter()
                    .find(|a| a.name.local_name == "id")
                    .map_or(String::new(), |a| a.value.clone());
                let mut chars = Vec::new();
                while let Ok(cat_event) = parser.next() {
                    match cat_event {
                        XmlEvent::Characters(content) => chars.push(content),
                        XmlEvent::EndElement { name: end_name }
                            if end_name.local_name == "category" =>
                        {
                            break
                        }
                        _ => {}
                    }
                }
                category_names.insert(id, chars.concat());
            }
            XmlEvent::StartElement { name, .. } => {
                current_element_name = name.local_name.clone();
            }
            XmlEvent::EndElement { name } if name.local_name == "offer" => {
                items.push(temp_item);
                inside_offer = false;
                temp_item = Item {
                    id: String::new(),
                    category_name: String::new(),
                    extra: HashMap::new(),
                    pictures: String::new(),
                };
            }
            XmlEvent::Characters(content) => {
                if inside_offer {
                    if &current_element_name == "categoryId" {
                        temp_item.category_name = category_names
                            .get(&content)
                            .unwrap_or(&String::from("Undefined"))
                            .clone();
                    } else if current_element_name != "picture" && current_element_name != "param" {
                        temp_item
                            .extra
                            .insert(current_element_name.replace(".", ","), content);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(items)
}

fn save_as_csv(items: Vec<Item>) -> Result<(), Box<dyn Error>> {
    let mut wtr = WriterBuilder::new()
        .delimiter(b';')
        .from_path("items.csv")?;

    for item in items {
        wtr.serialize(&item)?;
    }

    wtr.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let url = "https://divanboss.ru/yandex_mm.yml";
    let items = fetch_and_parse_xml(url)?;
    save_as_csv(items)?;
    Ok(())
}