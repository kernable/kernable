// Part 9: people sign in through the shop's API, and the kit decides what they may do.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

var api = cmp(os.Getenv("API_URL"), "http://localhost:3000")

// #region client
// A small client of the API: a person's access token and, inside the shop, its tenant.
type person struct{ email, token, tenant string }

type object = map[string]any

func call(method, path string, body object, as *person) (int, object) {
	var payload *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		payload = bytes.NewReader(raw)
	} else {
		payload = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, api+path, payload)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if as != nil && as.token != "" {
		req.Header.Set("Authorization", "Bearer "+as.token)
	}
	if as != nil && as.tenant != "" {
		req.Header.Set("X-Tenant", as.tenant)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	var out object
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

// signUp registers, confirms the email with the token from the mail (a file in development),
// signs in.
func signUp(name string) *person {
	email := strings.ToLower(name) + "@example.com"
	call("POST", "/register", object{"email": email, "name": name, "password": "correct horse battery"}, nil)
	token, _ := os.ReadFile("mail/" + email)
	call("POST", "/verify", object{"token": string(token)}, nil)
	status, signedIn := call("POST", "/sign-in", object{"email": email, "password": "correct horse battery"}, nil)
	fmt.Printf("%s signs up and signs in: %d\n", name, status)
	accessToken, _ := signedIn["accessToken"].(string)
	return &person{email: email, token: accessToken}
}

// #endregion client

func main() {
	// #region owner
	ann := signUp("Ann")
	opened, shop := call("POST", "/tenants", object{"slug": "anns-shop", "name": "Ann's shop"}, ann)
	tenant, _ := shop["tenant"].(string)
	ann.tenant = tenant
	fmt.Printf("Ann opens her shop: %d\n", opened)

	// Customers may place orders, and work only on orders they own.
	grouped, _ := call("POST", "/commands/group/group/CreateGroup", object{
		"aggregateId": "customers",
		"data": object{
			"name": "Customers",
			"permissions": []object{
				{"pattern": "shop.Place"},
				{"pattern": "shop.AddItem", "when": "owner"},
				{"pattern": "shop.Submit", "when": "owner"},
				{"pattern": "shop.GetOrder", "when": "owner"},
			},
		},
	}, ann)
	fmt.Printf("Ann creates the group Customers: %d\n", grouped)

	linked, link := call("POST", "/invitation-links", object{"groups": []object{{"group": "customers"}}, "label": "the shop's website"}, ann)
	fmt.Printf("Ann creates an invitation link: %d\n", linked)
	// #endregion owner

	// #region customers
	bob := signUp("Bob")
	cleo := signUp("Cleo")
	for _, p := range []struct {
		name string
		who  *person
	}{{"Bob", bob}, {"Cleo", cleo}} {
		joined, _ := call("POST", "/invitation-links/join", object{"token": link["token"]}, p.who)
		p.who.tenant = tenant
		fmt.Printf("%s joins through the link: %d\n", p.name, joined)
	}

	_, me := call("GET", "/me", nil, bob)
	placed, _ := call("POST", "/commands/shop/order/Place", object{"aggregateId": "o1", "data": object{"customerId": me["senderIdentity"]}}, bob)
	fmt.Printf("Bob places o1: %d\n", placed)
	added, _ := call("POST", "/commands/shop/order/AddItem", object{"aggregateId": "o1", "data": object{"sku": "apple", "qty": 2}}, bob)
	fmt.Printf("Bob adds two apples: %d\n", added)
	cleoAdds, cleoAddsBody := call("POST", "/commands/shop/order/AddItem", object{"aggregateId": "o1", "data": object{"sku": "pear", "qty": 1}}, cleo)
	fmt.Printf("Cleo adds to o1: %d %v\n", cleoAdds, cleoAddsBody["error"])

	bobReads, order := call("POST", "/queries/shop/GetOrder", object{"data": object{"id": "o1"}}, bob)
	items, _ := json.Marshal(order["items"])
	fmt.Printf("Bob reads o1: %d %v, %s\n", bobReads, order["status"], items)
	cleoReads, refused := call("POST", "/queries/shop/GetOrder", object{"data": object{"id": "o1"}}, cleo)
	fmt.Printf("Cleo reads o1: %d %v\n", cleoReads, refused["error"])
	// #endregion customers

	// #region rule
	_, cleoMe := call("GET", "/me", nil, cleo)
	forCleo, forCleoBody := call("POST", "/commands/shop/order/Place", object{"aggregateId": "o2", "data": object{"customerId": cleoMe["senderIdentity"]}}, bob)
	fmt.Printf("Bob places o2 in Cleo's name: %d %v\n", forCleo, forCleoBody["error"])
	// #endregion rule

	// #region explain
	_, why := call("POST", "/queries/group/Explain", object{
		"data": object{"identityId": cleoMe["senderIdentity"], "type": "shop.GetOrder", "aggregateType": "order", "aggregateId": "o1"},
	}, cleo)
	fmt.Printf("Why not? allowed: %v, %v\n", why["allowed"], why["code"])

	anonymous, nobody := call("POST", "/commands/shop/order/Place", object{"aggregateId": "o3", "data": object{}}, &person{tenant: tenant})
	fmt.Printf("Without signing in: %d %v\n", anonymous, nobody["error"])
	// #endregion explain
}

func cmp(v, otherwise string) string {
	if v != "" {
		return v
	}
	return otherwise
}
