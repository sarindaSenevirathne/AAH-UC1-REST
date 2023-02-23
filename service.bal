// import ballerina/graphql;
import ballerinax/mysql.driver as _;
import ballerinax/mysql;
import ballerina/http;
import ballerina/log;
import ballerina/sql;

configurable string USER = ?;
configurable string PASSWORD = ?;
configurable string HOST = ?;
configurable int PORT = ?;
configurable string DATABASE = ?;

public type Item record {|
    @sql:Column {name: "id"}
    int itemID;
    @sql:Column {name: "name"}
    string itemName;
    @sql:Column {name: "description"}
    string itemDesc;
    @sql:Column {name: "image"}
    string itemImage;
    StockDetails stockDetails;
|};

public type StockDetails record {|
    @sql:Column {name: "id"}
    int stockId;
    @sql:Column {name: "item_id"}
    int itemId;
    string includes;
    string intendedFor;
    string color;
    string material;
    decimal itemPrice;
    int quantity;
|};

public type CartEntry record {|
    int id;
    @sql:Column {name: "item_id"}
    int itemID;
    @sql:Column {name: "item_name"}
    string itemName;
    @sql:Column {name: "item_description"}
    string itemDesc;
    string includes;
    string intendedFor;
    string color;
    string material;
    @sql:Column {name: "item_price"}
    decimal itemPrice;
    int quantity;
    @sql:Column {name: "user_name"}
    string username;
|};

public type OrderRecord record {|
    int id;
    @sql:Column {name: "user_name"}
    string username;
    @sql:Column {name: "order_date"}
    string orderDate;
    @sql:Column {name: "card_number"}
    string cardNumber;
    @sql:Column {name: "tax_amount"}
    decimal taxAmount;
    @sql:Column {name: "shipping_amount"}
    decimal shippingAmount;
    @sql:Column {name: "total_amount"}
    decimal totalAmount;

    OrderEntry[] orderRecords;
|};

public type OrderEntry record {|
    int id;
    @sql:Column {name: "item_id"}
    int itemID;
    @sql:Column {name: "item_name"}
    string itemName;
    @sql:Column {name: "item_description"}
    string itemDesc;
    string includes;
    string intendedFor;
    string color;
    string material;
    decimal itemPrice;
    int quantity;
    @sql:Column {name: "order_entry_id"}
    int orderEntryId;
|};

public distinct service class ItemData {
    private final readonly & Item itemRecord;

    function init(Item itemRecord) {
        self.itemRecord = itemRecord.cloneReadOnly();
    }

    resource function get itemID() returns int {
        return self.itemRecord.itemID;
    }

    resource function get itemName() returns string {
        return self.itemRecord.itemName;
    }

    resource function get itemDesc() returns string {
        return self.itemRecord.itemDesc;
    }

    resource function get itemImage() returns string {
        return self.itemRecord.itemImage;
    }

    resource function get stockDetails() returns StockDetailsData {
        return new StockDetailsData(self.itemRecord.stockDetails);
    }
}

public distinct service class StockDetailsData {
    private final readonly & StockDetails stockDetailsRecord;

    function init(StockDetails stockDetailsRecord) {
        self.stockDetailsRecord = stockDetailsRecord.cloneReadOnly();
    }

    resource function get includes() returns string {
        return self.stockDetailsRecord.includes;
    }

    resource function get intendedFor() returns string {
        return self.stockDetailsRecord.intendedFor;
    }

    resource function get color() returns string {
        return self.stockDetailsRecord.color;
    }

    resource function get material() returns string {
        return self.stockDetailsRecord.material;
    }

    resource function get itemPrice() returns decimal {
        return self.stockDetailsRecord.itemPrice;
    }

    resource function get quantity() returns int {
        return self.stockDetailsRecord.quantity;
    }
}

final mysql:Client dbClient = check new (
    host = HOST, user = USER, password = PASSWORD, port = PORT, database = DATABASE, connectionPool = {maxOpenConnections: 3}
);

isolated function getItems() returns Item[]|error {
    log:printInfo("Retrieving items from database");
    stream<Item, sql:Error?> itemStream = dbClient->query(`SELECT * FROM ITEM`, Item);
    Item[] items = check from Item item in itemStream
        select item;
    foreach Item item in items {
        log:printInfo("Item: " + item.toString());
        StockDetails|sql:Error result = dbClient->queryRow(`SELECT * FROM STOCK_DETAIL WHERE item_id = ${item.itemID}`, StockDetails);
        item.stockDetails = check result;
    }
    return items;
}

isolated function deleteItem(int itemId) returns Item|error {
    Item item = check getItem(itemId);
    _ = check dbClient->execute(`DELETE FROM STOCK_DETAIL WHERE item_id = ${itemId}`);
    _ = check dbClient->execute(`DELETE FROM ITEM WHERE id = ${itemId}`);
    return item;
}

isolated function getItem(int itemId) returns Item|error {
    Item item = check dbClient->queryRow(`SELECT * FROM ITEM WHERE id = ${itemId}`, Item);
    StockDetails stockDetail = check dbClient->queryRow(`SELECT * FROM STOCK_DETAIL WHERE item_id = ${itemId}`, StockDetails);
    item.stockDetails = stockDetail;
    return item;
}

isolated function addItem(Item item) returns Item|error {
    transaction {
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO ITEM (name, description, image)
            VALUES (${item.itemName}, ${item.itemDesc}, ${item.itemImage})
            `);

        int|string? lastInsertId = result.lastInsertId;
        sql:ExecutionResult result2 = check dbClient->execute(`
            INSERT INTO STOCK_DETAIL (includes, intendedFor, color, material, itemPrice, quantity, item_id) 
            VALUES (${item.stockDetails.includes}, ${item.stockDetails.intendedFor}, ${item.stockDetails.color}, 
            ${item.stockDetails.material}, ${item.stockDetails.itemPrice}, ${item.stockDetails.quantity}, ${lastInsertId})
            `);

        if lastInsertId is int && result2.affectedRowCount == 1 {
            check commit;
            item.itemID = lastInsertId;
            return item;
        } else {
            rollback;
            return error("Unable to obtain last insert ID");
        }
    } on fail error e {
        // In case of error, the transaction block is rolled back automatically.
        if e is sql:DatabaseError {
            if e.detail().errorCode == 3819 {
                return error(string `Add item failure.`);
            }
        }
        return e;
    }
}

isolated function updateItem(Item item) returns Item|error {
    transaction {
        _ = check dbClient->execute(`
            UPDATE ITEM SET 
            name = ${item.itemName}, 
            description = ${item.itemDesc}, 
            image = ${item.itemImage}
            WHERE id = ${item.itemID}
            `);

        sql:ExecutionResult result2 = check dbClient->execute(`
            UPDATE STOCK_DETAIL SET 
            includes = ${item.stockDetails.includes}, 
            intendedFor = ${item.stockDetails.intendedFor}, 
            color = ${item.stockDetails.color}, 
            material = ${item.stockDetails.material}, 
            itemPrice = ${item.stockDetails.itemPrice}, 
            quantity = ${item.stockDetails.quantity}
            WHERE item_id = ${item.itemID}
            `);

        if result2.affectedRowCount == 1 {
            check commit;
            return item;
        } else {
            rollback;
            return error("Unable to obtain last insert ID");
        }
    } on fail error e {
        // In case of error, the transaction block is rolled back automatically.
        if e is sql:DatabaseError {
            if e.detail().errorCode == 3819 {
                return error(string `Update item failure.`);
            }
        }
        return e;
    }
}

isolated function getCartEntries(string username) returns CartEntry[]|error {
    log:printInfo("Retrieving cart items from database");
    stream<CartEntry, sql:Error?> cartEntryStream = dbClient->query(`SELECT * FROM CART_ENTRY WHERE user_name = ${username}`, CartEntry);
    return check from CartEntry cartEntry in cartEntryStream
        select cartEntry;
}

isolated function deleteCartItem(int id) returns CartEntry|error {
    CartEntry cartEntry = check getCartEntry(id);
    _ = check dbClient->execute(`DELETE FROM CART_ENTRY WHERE id = ${id}`);
    return cartEntry;
}

isolated function getCartEntry(int id) returns CartEntry|error {
    return check dbClient->queryRow(`SELECT * FROM CART_ENTRY WHERE id = ${id}`, CartEntry);
}

isolated function addCartEntry(CartEntry cartEntry) returns CartEntry|error {
    transaction {
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO CART_ENTRY (item_id, item_name, item_description, item_price, includes, intendedFor, color, material, quantity, user_name)
            VALUES (${cartEntry.itemID}, ${cartEntry.itemName}, ${cartEntry.itemDesc}, ${cartEntry.itemPrice}, ${cartEntry.includes}, ${cartEntry.intendedFor}, ${cartEntry.color}, ${cartEntry.material}, ${cartEntry.quantity}, ${cartEntry.username})
            `);

        int|string? lastInsertId = result.lastInsertId;
        if lastInsertId is int {
            check commit;
            cartEntry.id = lastInsertId;
            return cartEntry;
        } else {
            rollback;
            return error("Unable to obtain last insert ID");
        }
    } on fail error e {
        // In case of error, the transaction block is rolled back automatically.
        if e is sql:DatabaseError {
            if e.detail().errorCode == 3819 {
                return error(string `Add cart entry failure.`);
            }
        }
        return e;
    }
}

isolated function updateCartEntry(int id, int quantity) returns CartEntry|error {
    transaction {
        sql:ExecutionResult result = check dbClient->execute(`
            UPDATE CART_ENTRY SET 
            quantity = ${quantity}
            WHERE id = ${id}
            `);

        if result.affectedRowCount == 1 {
            check commit;
            return getCartEntry(id);
        } else {
            rollback;
            return error("Unable to obtain last insert ID");
        }
    } on fail error e {
        // In case of error, the transaction block is rolled back automatically.
        if e is sql:DatabaseError {
            if e.detail().errorCode == 3819 {
                return error(string `Update cart item failure.`);
            }
        }
        return e;
    }
}

isolated function addOrderRecord(OrderRecord orRec) returns OrderRecord|error {
    transaction {
        sql:ExecutionResult result = check dbClient->execute(`
            INSERT INTO ORDER_RECORD (user_name, order_date, card_number, tax_amount, shipping_amount, total_amount)
            VALUES (${orRec.username}, ${orRec.orderDate}, ${orRec.cardNumber}, ${orRec.taxAmount}, ${orRec.shippingAmount}, ${orRec.totalAmount})
            `);

        final int|string? lastInsertId = result.lastInsertId;
        if lastInsertId is int {
            foreach OrderEntry entry in orRec.orderRecords {
                _ = check dbClient->execute(`
                    INSERT INTO ORDER_ENTRY (item_id, item_name, item_description, includes, intendedFor, color, material, itemPrice, quantity, order_record_id)
                    VALUES (${entry.itemID}, ${entry.itemName}, ${entry.itemDesc}, ${entry.includes}, ${entry.intendedFor}, ${entry.color}, ${entry.material}, ${entry.itemPrice}, ${entry.quantity}, ${lastInsertId})
                    `);
                _ = check dbClient->execute(`
                    DELETE FROM CART_ENTRY WHERE id = ${entry.id}
                    `);
            }
            check commit;
            return orRec;
        } else {
            rollback;
            return error("Unable to obtain last insert ID");
        }
    } on fail error e {
        // In case of error, the transaction block is rolled back automatically.
        if e is sql:DatabaseError {
            if e.detail().errorCode == 3819 {
                return error(string `Add order record failure.`);
            }
        }
        return e;
    }
}

@display {
    label: "ItemService",
    id: "ItemService-10f2872b-1759-4573-ac6b-fed5f46fd849"
}

listener http:Listener httpListener = check new (9000);

// service / on new graphql:Listener(httpListener) {

//     // A resource method with `get` accessor inside a `graphql:Service` represents a field in the
//     // root `Query` type.
//     resource function get items() returns ItemData[]|error {
//         Item[] items = check getItems();

//         ItemData[] itemData = [];
//         foreach Item item in items {
//             itemData.push(new ItemData(item));
//         }
//         return itemData;
//     }

//     remote function addItem(@http:Payload Item item) returns ItemData|error {
//         Item itemEntry = check addItem(item);
//         return new ItemData(itemEntry);
//     }
// }

// service /admin on httpListener {
//     resource function get .() returns string {
//         return "Admin Service";
//     }

//     resource function get items() returns Item[]|error {
//         return check getItems();
//     }

//     resource function get items/[int id]() returns Item|error {
//         return getItem(id);
//     }

//     resource function post items(@http:Payload Item item) returns Item|error {
//         Item itemEntry = check addItem(item);
//         return itemEntry;
//     }

//     resource function put items(@http:Payload Item item) returns Item|error {
//         Item itemEntry = check updateItem(item);
//         return itemEntry;
//     }

//     resource function delete items/[int id]() returns Item|error {
//         return deleteItem(id);
//     }
// }

public type CartUpdate record {|
  int itemId;
  int quantity;
|};

public type OrderContent record {|
    string cardNumber;
    decimal taxAmount;
    decimal shippingAmount;
    decimal totalAmount;
|};

service /cart on httpListener {
    resource function get .() returns string {
        return "Cart Service";
    }

    resource function get items() returns CartEntry[]|error {
        return check getCartEntries("admin");
    }

    resource function post addItem(@http:Payload CartUpdate cu) returns CartEntry|error {
        Item item = check getItem(cu.itemId);
        CartEntry entry = {
            id: 0, 
            itemID: cu.itemId, 
            itemName: item.itemName, 
            itemDesc: item.itemDesc, 
            includes: item.stockDetails.includes, 
            intendedFor: item.stockDetails.intendedFor, 
            color: item.stockDetails.color, 
            material: item.stockDetails.material, 
            itemPrice: item.stockDetails.itemPrice, 
            quantity: cu.quantity, 
            username: "admin"};
        CartEntry cartEntry = check addCartEntry(entry);
        return cartEntry;
    }

    resource function post updateItemQuantity(@http:Payload CartUpdate cu) returns CartEntry|error {
        return check updateCartEntry(cu.itemId, cu.quantity);
    }

    resource function delete items/[int id]() returns CartEntry|error {
        return deleteCartItem(id);
    }

    resource function post checkout(@http:Payload OrderContent orEnt) returns OrderContent|error {

        OrderEntry[] orderEntries = [];
        foreach CartEntry entry in check getCartEntries("admin") {
            orderEntries.push({
                id: entry.id, 
                orderEntryId: 0, 
                itemID: entry.itemID, 
                itemName: entry.itemName, 
                itemDesc: entry.itemDesc, 
                includes: entry.includes, 
                intendedFor: entry.intendedFor, 
                color: entry.color, 
                material: entry.material, 
                itemPrice: entry.itemPrice, 
                quantity: entry.quantity
            });
        }
        _ = check addOrderRecord({
            id: 0, 
            username: "admin", 
            orderDate: "2021-01-01",
            cardNumber: orEnt.cardNumber, 
            taxAmount: orEnt.taxAmount, 
            shippingAmount: orEnt.shippingAmount, 
            totalAmount: orEnt.totalAmount,
            orderRecords: orderEntries
        });
        return orEnt;
    }

    resource function get admin/items() returns Item[]|error {
        return check getItems();
    }

    resource function get admin/items/[int id]() returns Item|error {
        return getItem(id);
    }

    resource function post admin/items(@http:Payload Item item) returns Item|error {
        Item itemEntry = check addItem(item);
        return itemEntry;
    }

    resource function put admin/items(@http:Payload Item item) returns Item|error {
        Item itemEntry = check updateItem(item);
        return itemEntry;
    }

    resource function delete admin/items/[int id]() returns Item|error {
        return deleteItem(id);
    }
}
