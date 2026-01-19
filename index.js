/**
 * AI Chief of Staff - Expired Listings Worker
 * Version 2.0 - Fixed Scoring + Tracerfy Skip Tracing
 * 
 * Features:
 * - HAR MLS CSV parsing
 * - 10-point urgency scoring rubric (differentiated scores)
 * - Tracerfy skip tracing for owner contact info
 * - Claude AI analysis for positioning
 * - D1 database storage
 * - Slack notifications with top 10 leads
 */

export default {
  async scheduled(event, env, ctx) {
    console.log("Cron triggered: Processing expired listings");
    await processExpiredListings(env);
  },

  async fetch(request, env, ctx) {
    if (request.method === "POST") {
      console.log("Manual trigger: Processing expired listings");
      ctx.waitUntil(processExpiredListings(env));
      return new Response("Processing started. Check Slack for results.", { status: 200 });
    }
    return new Response("AI Chief of Staff - Expired Listings Worker v2.0\nPOST to trigger processing.", { status: 200 });
  }
};

// ============================================
// MAIN PROCESSING FUNCTION
// ============================================

async function processExpiredListings(env) {
  try {
    console.log("Starting expired listings processing...");
    
    // 1. Get list of CSV files in R2 bucket
    const objects = await env.R2_BUCKET.list({ prefix: "expired-listings/" });
    const csvFiles = objects.objects.filter(obj => obj.key.endsWith(".csv"));
    console.log(`Found ${csvFiles.length} CSV files in R2 bucket`);
    
    if (csvFiles.length === 0) {
      console.log("No CSV files found");
      return;
    }
    
    // 2. Get already processed files from database
    const processedResult = await env.DB.prepare(
      `SELECT json_extract(summary, "$.csv_filename") as csv_filename 
       FROM intelligence WHERE topic_id = 1`
    ).all();
    const processedFiles = new Set(processedResult.results.map(r => r.csv_filename).filter(Boolean));
    console.log(`Already processed: ${processedFiles.size} files`);
    
    // 3. Find new files to process
    const newFiles = csvFiles.filter(f => !processedFiles.has(f.key));
    console.log(`New files to process: ${newFiles.length}`);
    
    if (newFiles.length === 0) {
      console.log("All files already processed");
      return;
    }
    
    let totalListings = 0;
    let allProcessedListings = [];
    
    // 4. Process each new file
    for (const file of newFiles) {
      console.log(`Processing file: ${file.key}`);
      
      const object = await env.R2_BUCKET.get(file.key);
      if (!object) continue;
      
      const csvText = await object.text();
      const listings = parseCSV(csvText);
      console.log(`Parsed ${listings.length} listings from ${file.key}`);
      
      // 5. Calculate urgency scores using rubric
      const scoredListings = listings.map(listing => ({
        ...listing,
        urgencyScore: calculateUrgencyScore(listing),
        csvFilename: file.key
      }));
      
      // 6. Skip trace with Tracerfy for owner contact info
      let enrichedListings = scoredListings;
      if (env.TRACERFY_API_KEY) {
        console.log("Starting Tracerfy skip tracing...");
        enrichedListings = await skipTraceWithTracerfy(scoredListings, env.TRACERFY_API_KEY);
        console.log(`Enriched ${enrichedListings.filter(l => l.ownerPhone || l.ownerEmail).length} listings with contact info`);
      }
      
      // 7. Get Claude analysis for top 20 (to save API costs)
      const sortedByScore = [...enrichedListings].sort((a, b) => b.urgencyScore - a.urgencyScore);
      const topListings = sortedByScore.slice(0, 20);
      
      console.log(`Analyzing top ${topListings.length} listings with Claude...`);
      for (let i = 0; i < topListings.length; i++) {
        const listing = topListings[i];
        console.log(`Analyzing ${i + 1}/${topListings.length}: ${listing.address}`);
        
        const analysis = await analyzeListingWithClaude(listing, env.ANTHROPIC_API_KEY);
        listing.analysis = analysis;
        
        // Store in database
        await storeIntelligence(env.DB, listing);
        
        // Rate limit: 1 second between Claude calls
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      // Store remaining listings (without Claude analysis) in database
      for (const listing of sortedByScore.slice(20)) {
        listing.analysis = { positioningAngle: "Lower priority - not analyzed", talkingPoints: [] };
        await storeIntelligence(env.DB, listing);
      }
      
      totalListings += listings.length;
      allProcessedListings = [...allProcessedListings, ...sortedByScore];
    }
    
    // 8. Send Slack summary with top 10
    console.log(`Sending Slack summary for ${totalListings} listings...`);
    try {
      await sendSlackSummary(
        env.SLACK_WEBHOOK,
        allProcessedListings.sort((a, b) => b.urgencyScore - a.urgencyScore).slice(0, 10),
        totalListings
      );
      console.log(`Processing complete. ${totalListings} listings analyzed.`);
    } catch (slackError) {
      console.error("Slack send failed:", slackError.message || slackError);
    }
  } catch (error) {
    console.error("Error in processExpiredListings:", error);
    throw error;
  }
}

// ============================================
// CSV PARSING (HAR MLS Format)
// ============================================

function parseCSV(csvText) {
  const lines = csvText.split("\n").filter(line => line.trim());
  if (lines.length < 2) return [];
  
  const headers = lines[0].split(",").map(h => h.trim().replace(/^"|"$/g, ""));
  const listings = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const listing = {};
    headers.forEach((header, index) => {
      listing[header] = values[index] || "";
    });
    
    // Build full address from HAR's split fields
    const addressParts = [
      listing["Street Number"],
      listing["Street Dir Prefix"],
      listing["Street Name"],
      listing["Street Suffix"],
      listing["Street Dir Suffix"],
      listing["Unit Number"]
    ].filter(p => p && p.trim()).join(" ");
    
    const normalized = {
      address: addressParts || "",
      city: listing["City/Location"] || "",
      zip: listing["Zip Code"] || "",
      state: "TX",
      price: listing["List Price"] || "",
      originalListDate: listing["List Date"] || "",
      expiredDate: listing["Last Change Timestamp"] || "",
      daysOnMarket: listing["DOM"] || "",
      cumulativeDaysOnMarket: listing["CDOM"] || "",
      bedrooms: listing["Bedrooms"] || "",
      bathrooms: listing["Baths Total"] || "",
      sqft: listing["Building SqFt"] || "",
      yearBuilt: listing["Year Built"] || "",
      listingAgent: listing["List Agent Full Name"] || "",
      listingOffice: listing["List Office Name"] || "",
      mlsNumber: listing["MLS Number"] || "",
      propertyType: listing["Property Type"] || "",
      status: listing["Status"] || ""
    };
    
    if (normalized.address) {
      listings.push(normalized);
    }
  }
  
  return listings;
}

function parseCSVLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === "," && !inQuotes) {
      values.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  values.push(current.trim());
  
  return values.map(v => v.replace(/^"|"$/g, ""));
}

// ============================================
// URGENCY SCORING RUBRIC (10-Point System)
// ============================================

function calculateUrgencyScore(listing) {
  let score = 0;
  
  const dom = parseInt(listing.daysOnMarket) || 0;
  const cdom = parseInt(listing.cumulativeDaysOnMarket) || 0;
  const price = parseInt(listing.price?.replace(/[,$]/g, "")) || 0;
  const expiredDate = listing.expiredDate ? new Date(listing.expiredDate) : null;
  const daysSinceExpired = expiredDate ? Math.floor((Date.now() - expiredDate.getTime()) / (1000 * 60 * 60 * 24)) : 999;
  
  // Factor 1: Motivation Level (based on DOM) - 0 to 2 points
  // Longer on market = more motivated seller
  if (dom >= 180) {
    score += 2;  // 6+ months = very motivated
  } else if (dom >= 90) {
    score += 1.5;  // 3-6 months = motivated
  } else if (dom >= 45) {
    score += 1;  // 45-90 days = somewhat motivated
  } else {
    score += 0.5;  // Under 45 days = may try again quickly
  }
  
  // Factor 2: Frustration Freshness - 0 to 2 points
  // Recently expired = fresh wound, better timing
  if (daysSinceExpired <= 3) {
    score += 2;  // Just expired = hot lead
  } else if (daysSinceExpired <= 7) {
    score += 1.5;  // Within a week = still fresh
  } else if (daysSinceExpired <= 14) {
    score += 1;  // 1-2 weeks = cooling
  } else if (daysSinceExpired <= 30) {
    score += 0.5;  // 2-4 weeks = cold
  }
  // 30+ days = 0 points
  
  // Factor 3: Repeat Failure (CDOM vs DOM) - 0 to 2 points
  // Multiple listing attempts = exhausted seller
  if (cdom > dom * 2) {
    score += 2;  // Multiple failed attempts
  } else if (cdom > dom * 1.5) {
    score += 1.5;  // Relisted at least once
  } else if (cdom > dom) {
    score += 1;  // Some prior history
  } else {
    score += 0.5;  // First attempt
  }
  
  // Factor 4: Price Point Appeal - 0 to 2 points
  // Under $200K = investor-friendly, easier sale
  if (price > 0 && price < 150000) {
    score += 2;  // Investor sweet spot
  } else if (price >= 150000 && price < 250000) {
    score += 1.5;  // First-time buyer range
  } else if (price >= 250000 && price < 400000) {
    score += 1;  // Move-up buyer range
  } else if (price >= 400000 && price < 600000) {
    score += 0.5;  // Higher end, smaller pool
  }
  // 600K+ = 0 points (luxury is harder)
  
  // Factor 5: Conversion Probability - 0 to 2 points
  // Based on property characteristics
  const yearBuilt = parseInt(listing.yearBuilt) || 2000;
  const propertyAge = new Date().getFullYear() - yearBuilt;
  
  if (propertyAge <= 15) {
    score += 1;  // Newer = easier to sell
  } else if (propertyAge <= 30) {
    score += 0.5;  // Middle age
  }
  // Older properties = 0 points
  
  // Bedroom count bonus
  const beds = parseInt(listing.bedrooms) || 0;
  if (beds >= 3 && beds <= 4) {
    score += 1;  // Sweet spot for families
  } else if (beds === 2 || beds === 5) {
    score += 0.5;  // Still appealing
  }
  // 1 bed or 6+ = 0 points
  
  // Round to 1 decimal place, cap at 10
  return Math.min(10, Math.round(score * 10) / 10);
}

// ============================================
// TRACERFY SKIP TRACING
// ============================================

async function skipTraceWithTracerfy(listings, apiKey) {
  try {
    // Build CSV for Tracerfy batch upload - include all required columns
    const csvHeader = "address,city,state,zip,first_name,last_name,mail_address,mail_city,mail_state\n";
    const csvRows = listings.map(l => 
      `"${l.address}","${l.city}","${l.state}","${l.zip}","","","","",""`
    ).join("\n");
    const csvContent = csvHeader + csvRows;
    
    // Create form data with CSV file and ALL required column mappings
    const formData = new FormData();
    formData.append("csv_file", new Blob([csvContent], { type: "text/csv" }), "listings.csv");
    formData.append("address_column", "address");
    formData.append("city_column", "city");
    formData.append("state_column", "state");
    formData.append("zip_column", "zip");
    formData.append("first_name_column", "first_name");
    formData.append("last_name_column", "last_name");
    formData.append("mail_address_column", "mail_address");
    formData.append("mail_city_column", "mail_city");
    formData.append("mail_state_column", "mail_state");
    
    // Submit batch to Tracerfy - CORRECT URL
    const submitResponse = await fetch("https://tracerfy.com/v1/api/trace/", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`
      },
      body: formData
    });
    
    if (!submitResponse.ok) {
      const errorText = await submitResponse.text();
      console.error("Tracerfy submit failed:", submitResponse.status, errorText);
      return listings; // Return original listings without enrichment
    }
    
    const submitResult = await submitResponse.json();
    const queueId = submitResult.queue_id || submitResult.id;
    console.log(`Tracerfy batch submitted, queue ID: ${queueId}`);
    
    // Poll for completion (max 2 minutes to avoid Cloudflare timeout)
    let attempts = 0;
    const maxAttempts = 12;
    let downloadUrl = null;

    while (attempts < maxAttempts) {
      await new Promise(resolve => setTimeout(resolve, 10000)); // Wait 10 seconds

      const statusResponse = await fetch(`https://tracerfy.com/v1/api/queue/${queueId}/`, {
        headers: {
          "Authorization": `Bearer ${apiKey}`
        }
      });

      if (!statusResponse.ok) {
        console.error("Tracerfy status check failed:", statusResponse.status);
        attempts++;
        continue;
      }

      const statusResult = await statusResponse.json();
      console.log(`Tracerfy status response:`, JSON.stringify(statusResult));

      // Handle various possible status field formats from Tracerfy API
      const status = statusResult.status || statusResult.state;
      const isComplete = status === "completed" || status === "complete" || status === "done" || statusResult.pending === false;
      const resultUrl = statusResult.download_url || statusResult.result_url || statusResult.file_url || statusResult.url;

      if (isComplete && resultUrl) {
        downloadUrl = resultUrl;
        break;
      }

      attempts++;
    }
    
    if (!downloadUrl) {
      console.error("Tracerfy timed out or no download URL");
      return listings;
    }
    
    // Download results CSV
    console.log(`Downloading results from: ${downloadUrl}`);
    const resultsResponse = await fetch(downloadUrl);
    if (!resultsResponse.ok) {
      console.error("Failed to download Tracerfy results");
      return listings;
    }
    
    const resultsCsv = await resultsResponse.text();
    const results = parseTracerfyResults(resultsCsv);
    console.log(`Parsed ${results.length} Tracerfy results`);
    
    // Match results back to listings by address
    const enriched = listings.map(listing => {
      const normalizedAddress = listing.address.toLowerCase().trim();
      const match = results.find(r => {
        const resultAddress = (r.address || "").toLowerCase().trim();
        return resultAddress.includes(normalizedAddress.split(" ")[0]) || 
               normalizedAddress.includes(resultAddress.split(" ")[0]);
      });
      
      if (match) {
        return {
          ...listing,
          ownerName: match.owner_name || match.first_name ? `${match.first_name || ""} ${match.last_name || ""}`.trim() : "",
          ownerPhone: match.mobile_1 || match.landline_1 || match.phone_1 || "",
          ownerEmail: match.email_1 || match.email || "",
          ownerMailingAddress: match.mail_address || ""
        };
      }
      return listing;
    });
    
    return enriched;
  } catch (error) {
    console.error("Tracerfy error:", error.message || error);
    return listings; // Return original listings on error
  }
}

// Parse Tracerfy CSV results
function parseTracerfyResults(csvText) {
  const lines = csvText.split("\n").filter(line => line.trim());
  if (lines.length < 2) return [];
  
  const headers = lines[0].split(",").map(h => h.trim().replace(/^"|"$/g, "").toLowerCase().replace(/\s+/g, "_"));
  const results = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || "";
    });
    results.push(row);
  }
  
  return results;
}

// ============================================
// CLAUDE AI ANALYSIS
// ============================================

async function analyzeListingWithClaude(listing, apiKey) {
  const prompt = `You are analyzing an expired listing for Bernard, a CPA and Realtor in Houston.

PROPERTY DETAILS:
- Address: ${listing.address}, ${listing.city}, TX ${listing.zip}
- Price: $${listing.price}
- Beds/Baths: ${listing.bedrooms}/${listing.bathrooms}
- SqFt: ${listing.sqft}
- Year Built: ${listing.yearBuilt}
- Days on Market: ${listing.daysOnMarket} (Cumulative: ${listing.cumulativeDaysOnMarket})
- Previous Agent: ${listing.listingAgent} at ${listing.listingOffice}
- Expired: ${listing.expiredDate}
${listing.ownerName ? `- Owner: ${listing.ownerName}` : ""}

URGENCY SCORE: ${listing.urgencyScore}/10

Provide a brief analysis in this exact format:

WHY IT DIDN'T SELL:
[2-3 specific reasons based on the data - be direct and practical]

BERNARD'S ANGLE:
[How he should position himself vs the previous agent - specific and actionable]

TALKING POINTS:
1. [First key point for initial call]
2. [Second key point]
3. [Third key point]`;

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01"
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 500,
        messages: [{ role: "user", content: prompt }]
      })
    });
    
    if (!response.ok) {
      console.error("Claude API error:", await response.text());
      return { positioningAngle: "Analysis failed", talkingPoints: [] };
    }
    
    const result = await response.json();
    const content = result.content[0].text;
    
    // Parse the structured response
    const angleMatch = content.match(/BERNARD'S ANGLE:\s*([\s\S]*?)(?=TALKING POINTS:|$)/i);
    const talkingPointsMatch = content.match(/TALKING POINTS:\s*([\s\S]*?)$/i);
    
    const talkingPoints = [];
    if (talkingPointsMatch) {
      const pointsText = talkingPointsMatch[1];
      const points = pointsText.match(/\d+\.\s*([^\n]+)/g);
      if (points) {
        points.forEach(p => {
          talkingPoints.push(p.replace(/^\d+\.\s*/, "").trim());
        });
      }
    }
    
    return {
      fullAnalysis: content,
      positioningAngle: angleMatch ? angleMatch[1].trim() : "",
      talkingPoints: talkingPoints
    };
  } catch (error) {
    console.error("Claude analysis error:", error);
    return { positioningAngle: "Analysis failed", talkingPoints: [] };
  }
}

// ============================================
// DATABASE STORAGE
// ============================================

async function storeIntelligence(db, listing) {
  const summary = {
    csv_filename: listing.csvFilename,
    address: listing.address,
    city: listing.city,
    zip: listing.zip,
    price: listing.price,
    beds: listing.bedrooms,
    baths: listing.bathrooms,
    sqft: listing.sqft,
    dom: listing.daysOnMarket,
    cdom: listing.cumulativeDaysOnMarket,
    previousAgent: listing.listingAgent,
    ownerName: listing.ownerName || null,
    ownerPhone: listing.ownerPhone || null,
    ownerEmail: listing.ownerEmail || null,
    analysis: listing.analysis
  };
  
  try {
    await db.prepare(
      `INSERT INTO intelligence (topic_id, title, summary, relevance_score, status, gathered_at)
       VALUES (?, ?, ?, ?, ?, datetime('now'))`
    ).bind(
      1, // topic_id for Expired Listings
      `${listing.address}, ${listing.city}`,
      JSON.stringify(summary),
      listing.urgencyScore,
      "new"
    ).run();
  } catch (error) {
    console.error("Database insert error:", error);
  }
}

// ============================================
// SLACK NOTIFICATION
// ============================================

async function sendSlackSummary(webhookUrl, topListings, totalCount) {
  const blocks = [
    {
      type: "header",
      text: {
        type: "plain_text",
        text: "🏠 Expired Listings Report",
        emoji: true
      }
    },
    {
      type: "section",
      text: {
        type: "mrkdwn",
        text: `*${totalCount} listings processed* | Top 10 by urgency score:`
      }
    },
    { type: "divider" }
  ];
  
  // Add each listing
  for (let i = 0; i < topListings.length; i++) {
    const listing = topListings[i];
    const hasContact = listing.ownerPhone || listing.ownerEmail;
    
    let contactInfo = "";
    if (listing.ownerPhone) {
      contactInfo += `📞 \`${listing.ownerPhone}\``;
    }
    if (listing.ownerEmail) {
      contactInfo += contactInfo ? " | " : "";
      contactInfo += `✉️ ${listing.ownerEmail}`;
    }
    if (!hasContact) {
      contactInfo = "⚠️ No contact info";
    }
    
    blocks.push({
      type: "section",
      text: {
        type: "mrkdwn",
        text: `*#${i + 1} - ${listing.address}, ${listing.city}*\n` +
              `Score: *${listing.urgencyScore}/10* | $${listing.price} | ${listing.bedrooms}bd/${listing.bathrooms}ba\n` +
              `DOM: ${listing.daysOnMarket} (CDOM: ${listing.cumulativeDaysOnMarket})\n` +
              contactInfo
      }
    });
    
    if (listing.analysis?.positioningAngle && listing.analysis.positioningAngle !== "Lower priority - not analyzed") {
      blocks.push({
        type: "context",
        elements: [{
          type: "mrkdwn",
          text: `💡 ${listing.analysis.positioningAngle.substring(0, 200)}${listing.analysis.positioningAngle.length > 200 ? "..." : ""}`
        }]
      });
    }
    
    blocks.push({ type: "divider" });
  }
  
  // Add action buttons
  blocks.push({
    type: "actions",
    elements: [
      {
        type: "button",
        text: {
          type: "plain_text",
          text: "View Full Report",
          emoji: true
        },
        url: "https://claude.ai/new?q=Show%20me%20today%27s%20expired%20listings",
        action_id: "view_report"
      }
    ]
  });
  
  try {
    const response = await fetch(webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ blocks })
    });
    
    if (!response.ok) {
      console.error("Slack notification failed:", await response.text());
    } else {
      console.log("Slack notification sent successfully");
    }
  } catch (error) {
    console.error("Slack error:", error);
  }
}
