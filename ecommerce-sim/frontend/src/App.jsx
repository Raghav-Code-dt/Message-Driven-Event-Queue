// src/App.jsx
import React, { useState, useEffect } from "react";
import { io } from "socket.io-client";

const SOCKET_URL = "http://localhost:4000";
const socket = io(SOCKET_URL, { transports: ["websocket"], autoConnect: true });

export default function App() {
  const [products, setProducts] = useState([]);
  const [cart, setCart] = useState([]); 
  const [orderStatus, setOrderStatus] = useState(null);
  const [conn, setConn] = useState("connecting");
  const [recommendations, setRecommendations] = useState({}); // ✅ NEW

  useEffect(() => {
    socket.on("connect", () => setConn("connected"));
    socket.on("disconnect", () => setConn("connecting"));
    socket.on("connect_error", () => setConn("error"));

    socket.on("products", (list) => Array.isArray(list) && setProducts(list));

    socket.on("stock:update", ({ productId, stock } = {}) => {
      setProducts((prev) =>
        prev.map((p) => (p.id === productId ? { ...p, stock } : p))
      );
    });

    socket.on("order:confirmed", ({ orderId, success, message } = {}) => {
      setOrderStatus({ orderId, success, message });
      if (success) setCart([]);
      setTimeout(() => setOrderStatus(null), 4000);
    });

    socket.on("order:shipped", ({ orderId } = {}) => {
      setOrderStatus({
        orderId,
        success: true,
        message: "Your order has been shipped 🚚",
      });
      setTimeout(() => setOrderStatus(null), 4000);
    });

    // ✅ Listen for recommendations
    socket.on("recommendation.update", ({ recommendations = {} } = {}) => {
      setRecommendations(recommendations);
    });

    return () => {
      socket.off();
    };
  }, []);

  const findProduct = (id) => products.find((p) => p.id === id);

  const addToCart = (product) => {
    if (!product || product.stock <= 0) return;

    setCart((prev) => {
      const exists = prev.find((c) => c.productId === product.id);
      return exists
        ? prev.map((c) =>
            c.productId === product.id
              ? { ...c, quantity: c.quantity + 1 }
              : c
          )
        : [...prev, { productId: product.id, quantity: 1 }];
    });

    socket.emit("cart:add", { productId: product.id });
  };

  const removeFromCart = (productId) => {
    const entry = cart.find((c) => c.productId === productId);
    if (!entry) return;

    setCart((prev) =>
      entry.quantity > 1
        ? prev.map((c) =>
            c.productId === productId
              ? { ...c, quantity: c.quantity - 1 }
              : c
          )
        : prev.filter((c) => c.productId !== productId)
    );

    socket.emit("cart:remove", { productId });
  };

  const checkout = () => {
    if (!cart.length) return;
    const items = cart.map(({ productId, quantity }) => {
      const p = findProduct(productId);
      return {
        productId,
        name: p?.name,
        price: p?.price ?? 0,
        quantity,
      };
    });
    const total = items.reduce((s, it) => s + it.price * it.quantity, 0);
    socket.emit("checkout", { items, total, timestamp: Date.now() });
  };

  const cartExpanded = cart.map((c) => {
    const p = findProduct(c.productId);
    return { ...c, name: p?.name, price: p?.price };
  });

  const total = cartExpanded.reduce((s, it) => s + it.price * it.quantity, 0);

  return (
    <div className="min-h-screen bg-gray-100 flex flex-col items-center p-8">
      <header className="w-full max-w-4xl flex items-center justify-between mb-4">
        <h1 className="text-3xl font-bold text-gray-800">🛒 Mini E-Commerce (Realtime)</h1>
        <span className={`px-3 py-1 rounded text-sm ${
          conn === "connected"
            ? "bg-emerald-600 text-white"
            : conn === "error"
            ? "bg-red-600 text-white"
            : "bg-yellow-600 text-white"
        }`}>
          {conn}
        </span>
      </header>

      <div className="w-full max-w-4xl grid grid-cols-2 gap-6">

        {/* PRODUCTS */}
        <div>
          <h2 className="text-xl font-semibold mb-3">Products</h2>
          <div className="space-y-4">
            {products.map((p) => (
              <div key={p.id} className="bg-white p-4 rounded-xl shadow space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <img src={p.img} className="w-16 h-16 rounded-md"/>
                    <div>
                      <div className="font-semibold">{p.name}</div>
                      <div className="text-sm text-gray-600">₹{p.price}</div>
                      <div className={`text-sm ${p.stock>0?"text-gray-500":"text-red-500"}`}>
                        Stock: {p.stock}
                      </div>
                    </div>
                  </div>
                  <button
                    onClick={() => addToCart(p)}
                    disabled={p.stock <= 0}
                    className={`px-3 py-1 rounded-lg text-white ${
                      p.stock > 0 ? "bg-blue-600 hover:bg-blue-700" : "bg-gray-400"
                    }`}
                  >
                    Add
                  </button>
                </div>

                {/* ✅ Recommendations UI */}
                {recommendations[p.id]?.length > 0 && (
                  <div className="bg-gray-50 p-2 rounded-lg border text-xs">
                    <div className="font-medium mb-1 text-gray-700">Users also bought:</div>
                    {recommendations[p.id].map((rec, idx) => (
                      <div key={idx} className="text-gray-600 flex justify-between">
                        <span>{rec.name}</span>
                        <span className="text-gray-400">×{rec.score}</span>
                      </div>
                    ))}
                  </div>
                )}

              </div>
            ))}
          </div>
        </div>

        {/* CART */}
        <div>
          <h2 className="text-xl font-semibold mb-3">Cart</h2>
          <div className="bg-white p-4 rounded-xl shadow max-h-80 overflow-auto space-y-3">
            {cartExpanded.length ? cartExpanded.map((it) => (
              <div key={it.productId} className="flex justify-between items-center border-b pb-2">
                <div>
                  <div className="font-medium">{it.name}</div>
                  <div className="text-sm text-gray-500">
                    Qty: {it.quantity} × ₹{it.price}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-2">
                  <div className="text-sm font-semibold">₹{it.price * it.quantity}</div>
                  <button onClick={() => removeFromCart(it.productId)} className="text-red-500 text-sm">
                    Remove
                  </button>
                </div>
              </div>
            )) : <p className="text-gray-500">Cart is empty</p> }
          </div>

          <div className="mt-4 p-3 bg-gray-800 text-white rounded-xl flex justify-between font-semibold">
            <span>Total:</span>
            <span>₹{total}</span>
          </div>

          <div className="mt-3 flex gap-3">
            <button
              onClick={checkout}
              disabled={!cartExpanded.length}
              className={`w-full py-2 rounded-lg font-semibold ${
                cartExpanded.length ? "bg-green-600" : "bg-gray-400"
              } text-white`}
            >
              Checkout
            </button>
            <button
              onClick={() => {
                cart.forEach((c) => {
                  for (let i = 0; i < c.quantity; i++) {
                    socket.emit("cart:remove", { productId: c.productId });
                  }
                });
                setCart([]);
              }}
              className="py-2 px-3 rounded-lg bg-red-500 text-white"
            >
              Clear
            </button>
          </div>

          {orderStatus && (
            <div className={`mt-3 p-3 rounded ${
              orderStatus.success ? "bg-emerald-600" : "bg-red-600"
            } text-white`}>
              Order #{orderStatus.orderId}: {orderStatus.message}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
