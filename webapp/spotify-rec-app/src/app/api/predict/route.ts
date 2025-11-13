import { NextRequest, NextResponse } from "next/server";

const MODEL_URL = process.env.MODEL_URL ?? "http://localhost:8000";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const response = await fetch(`${MODEL_URL}/predict`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text();
      console.error("Model service error:", text);
      return NextResponse.json(
        { error: "Prediction service failed" },
        { status: 502 },
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error("Predict API error:", error);
    return NextResponse.json(
      { error: "Unable to fetch prediction" },
      { status: 500 },
    );
  }
}
