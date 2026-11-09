//! Part 9: people sign in through the shop's API, and the kit decides what they may do.
use kernable::BoxError;
use serde_json::{Value, json};

// #region client
/// A small client of the API: a person's access token and, inside the shop, its tenant.
#[derive(Clone, Default)]
struct Person {
    token: Option<String>,
    tenant: Option<String>,
}

struct Client {
    http: reqwest::Client,
    api: String,
}

impl Client {
    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        person: &Person,
    ) -> Result<(u16, Value), BoxError> {
        let url = format!("{}{path}", self.api);
        let mut request = match method {
            "GET" => self.http.get(url),
            _ => self.http.post(url),
        };
        if let Some(token) = &person.token {
            request = request.bearer_auth(token);
        }
        if let Some(tenant) = &person.tenant {
            request = request.header("x-tenant", tenant);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let res = request.send().await?;
        let status = res.status().as_u16();
        let body = if status == 204 {
            Value::Null
        } else {
            res.json().await?
        };
        Ok((status, body))
    }

    /// Registers, confirms the email with the token from the mail (a file in development), signs in.
    async fn sign_up(&self, name: &str) -> Result<Person, BoxError> {
        let email = format!("{}@example.com", name.to_lowercase());
        let password = "correct horse battery";
        let nobody = Person::default();
        let register = json!({ "email": email, "name": name, "password": password });
        self.call("POST", "/register", Some(register), &nobody)
            .await?;
        let token = std::fs::read_to_string(format!("mail/{email}"))?;
        self.call("POST", "/verify", Some(json!({ "token": token })), &nobody)
            .await?;
        let sign_in = json!({ "email": email, "password": password });
        let (status, signed_in) = self
            .call("POST", "/sign-in", Some(sign_in), &nobody)
            .await?;
        println!("{name} signs up and signs in: {status}");
        Ok(Person {
            token: signed_in["accessToken"].as_str().map(str::to_owned),
            tenant: None,
        })
    }
}
// #endregion client

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let api = std::env::var("API_URL").unwrap_or_else(|_| "http://localhost:3000".into());
    let client = Client {
        http: reqwest::Client::new(),
        api,
    };

    // #region owner
    let mut ann = client.sign_up("Ann").await?;
    let open = json!({ "slug": "anns-shop", "name": "Ann's shop" });
    let (opened, shop) = client.call("POST", "/tenants", Some(open), &ann).await?;
    let tenant = shop["tenant"].as_str().map(str::to_owned);
    ann.tenant = tenant.clone();
    println!("Ann opens her shop: {opened}");

    // Customers may place orders, and work only on orders they own.
    let group = json!({
        "aggregateId": "customers",
        "data": {
            "name": "Customers",
            "permissions": [
                { "pattern": "shop.Place" },
                { "pattern": "shop.AddItem", "when": "owner" },
                { "pattern": "shop.Submit", "when": "owner" },
                { "pattern": "shop.GetOrder", "when": "owner" },
            ],
        },
    });
    let (grouped, _) = client
        .call(
            "POST",
            "/commands/group/group/CreateGroup",
            Some(group),
            &ann,
        )
        .await?;
    println!("Ann creates the group Customers: {grouped}");

    let new_link = json!({ "groups": [{ "group": "customers" }], "label": "the shop's website" });
    let (linked, link) = client
        .call("POST", "/invitation-links", Some(new_link), &ann)
        .await?;
    println!("Ann creates an invitation link: {linked}");
    // #endregion owner

    // #region customers
    let mut bob = client.sign_up("Bob").await?;
    let mut cleo = client.sign_up("Cleo").await?;
    for (name, person) in [("Bob", &mut bob), ("Cleo", &mut cleo)] {
        let join = json!({ "token": link["token"] });
        let (joined, _) = client
            .call("POST", "/invitation-links/join", Some(join), person)
            .await?;
        person.tenant = tenant.clone();
        println!("{name} joins through the link: {joined}");
    }

    let (_, me) = client.call("GET", "/me", None, &bob).await?;
    let place = json!({ "aggregateId": "o1", "data": { "customerId": me["senderIdentity"] } });
    let (placed, _) = client
        .call("POST", "/commands/shop/order/Place", Some(place), &bob)
        .await?;
    println!("Bob places o1: {placed}");
    let add =
        |sku: &str, qty: i64| json!({ "aggregateId": "o1", "data": { "sku": sku, "qty": qty } });
    let (added, _) = client
        .call(
            "POST",
            "/commands/shop/order/AddItem",
            Some(add("apple", 2)),
            &bob,
        )
        .await?;
    println!("Bob adds two apples: {added}");
    let (cleo_adds, cleo_adds_body) = client
        .call(
            "POST",
            "/commands/shop/order/AddItem",
            Some(add("pear", 1)),
            &cleo,
        )
        .await?;
    println!(
        "Cleo adds to o1: {cleo_adds} {}",
        text(&cleo_adds_body["error"])
    );

    let get = json!({ "data": { "id": "o1" } });
    let (bob_reads, order) = client
        .call("POST", "/queries/shop/GetOrder", Some(get.clone()), &bob)
        .await?;
    println!(
        "Bob reads o1: {bob_reads} {}, {}",
        text(&order["status"]),
        order["items"]
    );
    let (cleo_reads, refused) = client
        .call("POST", "/queries/shop/GetOrder", Some(get), &cleo)
        .await?;
    println!("Cleo reads o1: {cleo_reads} {}", text(&refused["error"]));
    // #endregion customers

    // #region rule
    let (_, cleo_me) = client.call("GET", "/me", None, &cleo).await?;
    let for_cleo =
        json!({ "aggregateId": "o2", "data": { "customerId": cleo_me["senderIdentity"] } });
    let (status, body) = client
        .call("POST", "/commands/shop/order/Place", Some(for_cleo), &bob)
        .await?;
    println!(
        "Bob places o2 in Cleo's name: {status} {}",
        text(&body["error"])
    );
    // #endregion rule

    // #region explain
    let explain = json!({
        "data": {
            "identityId": cleo_me["senderIdentity"],
            "type": "shop.GetOrder",
            "aggregateType": "order",
            "aggregateId": "o1",
        },
    });
    let (_, why) = client
        .call("POST", "/queries/group/Explain", Some(explain), &cleo)
        .await?;
    println!(
        "Why not? allowed: {}, {}",
        why["allowed"],
        text(&why["code"])
    );

    let anonymous = Person {
        token: None,
        tenant: tenant.clone(),
    };
    let place = json!({ "aggregateId": "o3", "data": {} });
    let (status, nobody) = client
        .call(
            "POST",
            "/commands/shop/order/Place",
            Some(place),
            &anonymous,
        )
        .await?;
    println!("Without signing in: {status} {}", text(&nobody["error"]));
    // #endregion explain
    Ok(())
}

/// A JSON string as text, without quotes.
fn text(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}
