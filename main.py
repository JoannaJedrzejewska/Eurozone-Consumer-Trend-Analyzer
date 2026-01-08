import asyncio
import sys
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, IntPrompt
from gateway import CESDataGateway
from engine import AnalyticsEngine, GenericMeanStrategy
import plotext as plt


async def main():
    console = Console()
    gateway = CESDataGateway("ces_data.csv")
    engine = AnalyticsEngine()

    console.print(Panel.fit("[bold green]ECB Consumer Expectations Information System [/bold green]"))
    with console.status("[bold yellow]Loading Data...") as status:
        data = await gateway.load_all_data()

    valid_years = engine.get_available_years(data)
    min_y, max_y = valid_years[0], valid_years[-1]

    while True:
        console.print("\n[bold blue]Analytical Operations:[/bold blue]")
        console.print("1. Global Economic Snapshot")
        console.print("2. Yearly Inflation Trends")
        console.print("3. Search by Variable & Year Range")
        console.print("4. Deep-Dive Respondent ID")
        console.print("5. [bold magenta]Generate Trend Graph[/bold magenta]")
        console.print("6. [bold yellow]Data Quality (N/A) Report[/bold yellow]")
        console.print("7. [bold red]Exit[/bold red]")
        
        choice = Prompt.ask("Action", choices=["1", "2", "3", "4", "5", "6", "7"])

        if choice == "1":
            summary_table = Table(title="Global Mean Values")
            summary_table.add_column("Indicator", style="cyan")
            summary_table.add_column("Value", justify="right", style="bold green")

            tasks = [
                engine.run_analysis(data, GenericMeanStrategy("macro.inflation_1y")),
                engine.run_analysis(data, GenericMeanStrategy("consumption.income_growth")),
                engine.run_analysis(data, GenericMeanStrategy("labor.job_loss_prob")),
                engine.run_analysis(data, GenericMeanStrategy("housing.house_price_exp"))
            ]
            results = await asyncio.gather(*tasks)
            indicators = ["Inflation (1y)", "Income Growth", "Job Loss Balance", "House Price Exp"]
            for name, val in zip(indicators, results):
                summary_table.add_row(name, f"{val:.2f}%")
            console.print(summary_table)

        elif choice == "2":
            yearly_stats = await engine.get_yearly_report(data)
            table = Table(title="Inflation Trends")
            table.add_column("Year", style="cyan")
            table.add_column("Mean Inflation", style="magenta")
            for year in sorted(yearly_stats.keys()):
                table.add_row(str(year), f"{yearly_stats[year]:.2f}%")
            console.print(table)

        elif choice == "3":
            var_name = Prompt.ask("Enter Variable Code (e.g., c4030) or Name (e.g., income)").lower()
            path = gateway.VARIABLE_MAP.get(var_name, var_name)
            
            s_year = IntPrompt.ask(f"Start Year (Dataset range: {min_y}-{max_y})")
            e_year = IntPrompt.ask(f"End Year (Dataset range: {min_y}-{max_y})")

            if s_year not in valid_years or e_year not in valid_years:
                console.print(f"[bold red]Error:[/bold red] Data for {s_year} or {e_year} is not here.")
                console.print(f"Please try a year within the actual range: [bold cyan]{min_y} to {max_y}[/bold cyan]")
                continue

            filtered_data = engine.filter_by_date(data, s_year, e_year)
            result = await engine.run_analysis(filtered_data, GenericMeanStrategy(path))
            console.print(Panel(f"Analysis for [bold]{var_name}[/bold] ({s_year}-{e_year}): [bold green]{result:.4f}%[/bold green]"))

        elif choice == "4":
            console.print("\n[bold yellow]Data Deep-Dive (Monthly Aggregates)[/bold yellow]")
            console.print("Format examples: [cyan]'2023-03'[/cyan], [cyan]'03-2023'[/cyan], or [cyan]'March 2023'[/cyan]")
            
            date_query = Prompt.ask("Enter Month and Year")
            obs = engine.find_by_date(data, date_query)
            
            if obs:
                console.print(f"\n[bold underline cyan]REPORT FOR: {obs.observation_date.strftime('%B %Y')}[/bold underline cyan]")
                
                def safe_val(val, unit="%"):
                    return f"[bold]{val:.2f}{unit}[/bold]" if val is not None else "[dim]N/A[/dim]"

                indicators = [
                    f"Inflation (1y): [bold magenta]{safe_val(obs.macro.inflation_1y)}[/bold magenta]",
                    f"Unemployment Perception: [bold yellow]{safe_val(obs.macro.unemployment_percept)}[/bold yellow]",
                    f"Expected Income Growth: [bold green]{safe_val(obs.consumption.income_growth)}[/bold green]"
                ]
                console.print(Panel("\n".join(indicators), title="Core Economic Means"))

                if obs.demographics.household_members:
                    table = Table(title="Household Variable Components", header_style="bold white on blue")
                    table.add_column("Member Slot", justify="center")
                    table.add_column("Mean Age", justify="right", style="green")
                    table.add_column("Relation Code", justify="right", style="blue")
                    
                    for m in obs.demographics.household_members:
                        age_display = f"{m.age:.2f}" if m.age is not None else "[dim]N/A[/dim]"
                        rel_display = f"{m.relation_status:.2f}" if m.relation_status is not None else "[dim]N/A[/dim]"
                        
                        table.add_row(str(m.member_id), age_display, rel_display)
                    console.print(table)
            else:
                console.print(f"[bold red]Error:[/bold red] No data found for '{date_query}'.")

        elif choice == "5":
            var_name = Prompt.ask("Variable (e.g., inflation, income, c2150_1, c1150_6)").lower()
            path = gateway.VARIABLE_MAP.get(var_name, var_name)
            
            with console.status(f"[bold magenta]Drawing graph..."):
                x, y = await engine.get_time_series(data, path)
            
            if not x or all(v == 0 for v in y):
                console.print("[red]No data found for this variable in the dataset.[/red]")
                continue

            plt.clf()
            plt.date_form('Y-m')
            plt.plot(x, y, marker="dot", color="magenta")
            plt.title(f"Trend for {var_name.upper()}")
            plt.show()
            
        elif choice == "6":
            quality_results = await engine.run_quality_report(data)
            table = Table(title="Personal Data Missingness (N/A) Report")
            table.add_column("Category", style="cyan")
            table.add_column("Missing (%)", justify="right", style="bold red")

            for cat, percent in quality_results.items():
                table.add_row(cat, f"{percent:.2f}%")
            
            console.print(table)
            console.print("[dim italic]*Higher percentages mean respondents chose not to answer personal questions that month.[/dim italic]")

        elif choice == "7":
            sys.exit()

if __name__ == "__main__":
    asyncio.run(main())